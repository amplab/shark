/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore2.column

import shark.{LogHelper, SharkConfVars}
import java.nio.ByteBuffer
import java.nio.ByteOrder

import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.io.Text

import collection.mutable._
import collection.mutable.ListBuffer

class StringColumnBuilder extends ColumnBuilder[Text] with LogHelper {

  private var _stats: ColumnStats.StringColumnStats = null
  private var _uniques: collection.mutable.Set[Text] = new HashSet()

  // In string, a length of -1 is used to represent null values.
  private val NULL_VALUE = -1
  private var _arr: ByteArrayList = null
  private var _lengthArr: IntArrayList = null

  // build run length encoding optimistically
  private var rleSs = new RLEStreamingSerializer[Text]( { () => null })

  override def initialize(initialSize: Int) {
    _arr = new ByteArrayList(initialSize * ColumnIterator.STRING_SIZE)
    _lengthArr = new IntArrayList(initialSize)
    _stats = new ColumnStats.StringColumnStats
    logInfo("initialized a StringColumnStats ")
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
      append(v)
    }
  }

  override def append(v: Text) {
    _lengthArr.add(v.getLength)
    _arr.addElements(_arr.size, v.getBytes, 0, v.getLength)
    _stats.append(v)
    _uniques += v
  }

  override def appendNull() {
    _lengthArr.add(NULL_VALUE)
    _stats.appendNull()
  }

  override def stats = _stats

  def pickCompressionScheme: String = {
    // Initial RLE choice logic - use RLE if the
    // selectivity is > 20% &&
    // ratio of transitions > 30% &&
    // #uniques is under 10000 (avoid stack overflows)
    val selectivity = (_uniques.size).toDouble / _lengthArr.size
    val transitionsRatio = (_stats.transitions).toDouble / _lengthArr.size
    val rleUsed = 
      ((selectivity < 0.2) &&
        (transitionsRatio < 0.3) &&
        (_uniques.size < 10000))
    logInfo("uniques=" + _uniques.size + " selectivity=" + selectivity + 
      " transitionsRatio=" + transitionsRatio + 
      " transitions=" + _stats.transitions +
      " #values=" + _lengthArr.size)

    if(rleUsed) "RLE"
    else "none"
  }


  override def build: ByteBuffer = {

    var scheme = System.getenv("TEST_SHARK_COLUMN_COMPRESSION_SCHEME")
    if(scheme == null || scheme == "auto") scheme = pickCompressionScheme
    // choices are none, auto, RLE, LZF

    scheme.toUpperCase match {
      case "NONE" => {
        var minbufsize = _lengthArr.size * 4 + _arr.size +
        ColumnIterator.COLUMN_TYPE_LENGTH
        val buf = ByteBuffer.allocate(minbufsize)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + minbufsize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.STRING)

        populateStringsInBuffer(_arr, _lengthArr, buf)
      }
      case "LZF" => {

        // on waking up - craft a byte array and send it for LZF compr
        val tempBufSize = _lengthArr.size * 4 + _arr.size
        val tempBuf = ByteBuffer.allocate(tempBufSize)
        logInfo("Allocated tempBuf of size " + tempBufSize)
        tempBuf.order(ByteOrder.nativeOrder())
        populateStringsInBuffer(_arr, _lengthArr, tempBuf)
        tempBuf.rewind
        val arr: Array[Byte] = tempBuf.array()

        val compressed = LZFSerializer.encode(arr)

        var minbufsize = 4 + compressed.size +
        ColumnIterator.COLUMN_TYPE_LENGTH
        val buf = ByteBuffer.allocate(minbufsize)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + minbufsize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.LZF_STRING)

        LZFSerializer.writeToBuffer(buf, compressed)
        buf.rewind
        buf
      }
      case "RLE" => {
        // var strings = new ListBuffer[Text]()
        var totalStringLengthInBuffer = 0

        var i = 0
        var runningOffset = 0
        while (i < _lengthArr.size) {
          if (NULL_VALUE != _lengthArr.get(i)) {
            val writable = new Text()
            writable.append(_arr.elements(), runningOffset, _lengthArr.get(i))
            rleSs.encodeSingle(writable)
            // println(writable.toString + " len " + _lengthArr.get(i))
            totalStringLengthInBuffer += (_lengthArr.get(i) + 4)
            runningOffset += _lengthArr.get(i)
          } else {
            rleSs.encodeSingle(null)
            // println( " len " + _lengthArr.get(i))
            totalStringLengthInBuffer += 4
          }
          i += 1
        }
        // alternative recursive call for encode in bulk
        // val rleStrings = RLESerializer.encode(strings.toList)
        
        // streaming construction
        val rleStrings = rleSs.getCoded

        totalStringLengthInBuffer = 0
        var runs = new ArrayBuffer[Int]()
        rleStrings.foreach { x =>
          val (run, value) = x
          runs += run
          totalStringLengthInBuffer += 4 // int to mark length
          if(value != null)
            totalStringLengthInBuffer += value.getLength()
        }

        var minbufsize = rleStrings.size*4 + //runs
        rleStrings.size*4 + //lengths per string
        totalStringLengthInBuffer         + //string
        ColumnIterator.COLUMN_TYPE_LENGTH
        logInfo("number of Strings " + rleStrings.size + " totalStringLengthInBuffer  " + totalStringLengthInBuffer)

        val buf = ByteBuffer.allocate(minbufsize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.RLE_STRING)

        logInfo("Allocated ByteBuffer (RLE) of size " + minbufsize)
        logInfo("size of runs " + runs.size)
        RLESerializer.writeToBuffer(buf, runs.toList)
        populateStringsInBuffer(rleStrings, buf)
      }
      case _ => throw new IllegalArgumentException(
        "scheme muse be one of auto, none, RLE, LZF")
    } // match
  }

  protected def populateStringsInBuffer(_arr:ByteArrayList,
    _lengthArr:IntArrayList, buf:ByteBuffer): ByteBuffer = {

    var i = 0
    var runningOffset = 0
    while (i < _lengthArr.size) {
      val len = _lengthArr.get(i)
      buf.putInt(len)

      if (NULL_VALUE != len) {
        buf.put(_arr.elements(), runningOffset, len)
        runningOffset += len
      }

      i += 1
    }

    buf.rewind
    buf
  }

  // for encoded pairs of (length, value)
  protected def populateStringsInBuffer(l: List[(Int, Text)],
    buf: ByteBuffer): ByteBuffer = {

    var i = 0
    var runningOffset = 0

    val iter = l.iterator

    while (iter.hasNext) {
      val (run, value) = iter.next
      var len = NULL_VALUE
      if (value != null) {
        len = value.getLength()
      }

      buf.putInt(len)
      runningOffset += 1

      if (NULL_VALUE != len && 0 != len) {
        buf.put(value.getBytes(), 0, len)
        runningOffset += len
      }
    }

    buf.rewind
    buf
  }

}
