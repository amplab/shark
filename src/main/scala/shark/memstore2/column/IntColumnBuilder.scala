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

import com.ning.compress.lzf.LZFEncoder
import java.nio.ByteBuffer
import java.nio.ByteOrder

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.ints.IntArraySet

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector

import collection.mutable.{Set, HashSet}
import shark.LogHelper
import shark.memstore2.column.CompressionScheme._

/** Build a column of Int values into a ByteBuffer in memory.
  */
class IntColumnBuilder extends ColumnBuilder[Int] with LogHelper{
  private var _stats: ColumnStats.IntColumnStats = new ColumnStats.IntColumnStats
  private var _nonNulls: IntArrayList = null

  // In the worst case this Set can be as big as the column itself. This huge waste of
  // memory should be replaced by HLL or a shortcut eventually. Leaving it here for now as we
  // figure out the compression scheme heuristics.
  private var uniques = new IntArraySet(0)

  private val MAX_DICT_UNIQUE_VALUES = 256 // 2 ** 8 - storable in 8 bits or 1 Byte

  override def initialize(initialSize: Int) {
    _nonNulls = new IntArrayList(initialSize)
    _stats = new ColumnStats.IntColumnStats
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[IntObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Int) {
    _nonNulls.add(v)
    _stats.append(v)
    uniques.add(v)
  }

  override def appendNull() {
    _nullBitmap.set(_nonNulls.size + _stats.nullCount)
    _stats.appendNull()
  }

  override def stats = _stats

  def pickCompressionScheme: CompressionScheme.Value = {
    // RLE choice logic - use RLE if the
    // selectivity is < 20% &&
    // ratio of transitions < 50% (run+value is 2 ints instead of 1)
    val selectivity = (uniques.size).toDouble / _nonNulls.size
    val transitionsRatio = (_stats.transitions).toDouble / _nonNulls.size

    if (selectivity > 0.2) {

      if (uniques.size < MAX_DICT_UNIQUE_VALUES) {
        Dict
      } else {
        CompressionScheme.None // compressed data might be bigger than original - don't bother
                               // trying
      }

    } else if (transitionsRatio < 0.5) {
      RLE
    } else if (uniques.size < MAX_DICT_UNIQUE_VALUES) {
      Dict
    } else {
      LZF // low selectivity, but large number of uniques - LZF might offer good trade-off
    }
  }


  /** After all values have been append()ed, build() is called to write all the
    * values into a ByteBuffer.
    */
  override def build: ByteBuffer = {
    // store count in stats object so it can be used later for optimization
    _stats.uniqueCount = uniques.size
    logDebug("scheme at the start of build() was " + scheme)

    // highest priority override is if someone (like a test) calls the getter
    //.scheme()

    // next priority override - from TBL PROPERTIES

    if(scheme == null || scheme == CompressionScheme.Auto) scheme = pickCompressionScheme
    // choices are none, auto, RLE, dict

    val selectivity = (uniques.size).toDouble / _nonNulls.size
    val transitionsRatio = (_stats.transitions).toDouble / _nonNulls.size
    logInfo(
      "uniques=" + uniques.size + 
      " selectivity=" + selectivity +
      " transitionsRatio=" + transitionsRatio + 
      " transitions=" + _stats.transitions +
      " #values=" + _nonNulls.size)


    scheme match {
      case None => {
        val minbufsize = (_nonNulls.size*4) + ColumnIterator.COLUMN_TYPE_LENGTH + sizeOfNullBitmap
        val buf = ByteBuffer.allocate(minbufsize)
        logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
          ColumnIterator.COLUMN_TYPE_LENGTH +
          " _nonNulls.size*4 " + _nonNulls.size*4)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + minbufsize)

        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.INT)

        writeNullBitmap(buf)

        var i = 0
        while (i < _nonNulls.size) {
          buf.putInt(_nonNulls.get(i))
          i += 1
        }
        buf.rewind()
        buf
      } 
      case LZF => {
        val (tempBufSize, compressedByteArray) = encodeAsLZFBlocks(_nonNulls)
        val minbufsize = (compressedByteArray.size*1) + 2*4 + ColumnIterator.COLUMN_TYPE_LENGTH +
          sizeOfNullBitmap
        logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
          ColumnIterator.COLUMN_TYPE_LENGTH +
          " 2*4 (numUncompressedBytes, length) " + 2*4 +
          " compressedByteArray.size*1 " + compressedByteArray.size*1)

        val buf = ByteBuffer.allocate(minbufsize)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + minbufsize)

        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.LZF_INT)

        writeNullBitmap(buf)

        LZFSerializer.writeToBuffer(buf, _nonNulls.size*4, compressedByteArray)

        buf.rewind()
        buf
      }
      case RLE => {
        var rleSs = new RLEStreamingSerializer[Int]( { () => -1 } )
        var i = 0
        while (i < _nonNulls.size) {
          rleSs.encodeSingle(_nonNulls.get(i))
          i += 1
        }

        val rleStrings = rleSs.getCoded
        val vals = rleStrings map (_._2)
        val runs = new IntArrayList(vals.size)
        rleStrings.foreach ( x => runs.add(x._1) )

        val minbufsize = 
          4 + //#runs
          runs.size*4 + //runs
          runs.size*4 + //values
          ColumnIterator.COLUMN_TYPE_LENGTH +
          sizeOfNullBitmap

        val buf = ByteBuffer.allocate(minbufsize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.RLE_INT)

        logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
          ColumnIterator.COLUMN_TYPE_LENGTH +
          " runs.size*8 " + runs.size*8)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + minbufsize)

        writeNullBitmap(buf)
        RLESerializer.writeToBuffer(buf, runs)

        // after writing runs, write values
        vals.foreach { 
          buf.putInt(_)
        }

        buf.rewind()
        buf
      }
      case Dict => {
        val dict = new IntDictionary
        logInfo("#uniques " + uniques.size)
        dict.initialize(uniques.toIntArray)
        val minbufsize = (_nonNulls.size*1) + ColumnIterator.COLUMN_TYPE_LENGTH +
        sizeOfNullBitmap + (dict.sizeInBits/8)

        logInfo(
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
            ColumnIterator.COLUMN_TYPE_LENGTH +
            " sizeOfNullBitmap " + sizeOfNullBitmap +
            " dict.sizeinbits/8 " + (dict.sizeInBits/8) +
            " _nonNulls.size*1 " + _nonNulls.size*1)

        val buf = ByteBuffer.allocate(minbufsize)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + minbufsize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.DICT_INT)

        writeNullBitmap(buf)

        DictionarySerializer.writeToBuffer(buf, dict)

        var i = 0
        while (i < _nonNulls.size) {
          buf.put(dict.getByte(_nonNulls.get(i)))
          i += 1
        }
        logInfo("Compression ratio is " + (_nonNulls.size.toFloat*4/(minbufsize)) + " : 1")
        buf.rewind()
        buf
      }
      case _ => throw new IllegalArgumentException(
        "scheme must be one of Auto, None, LZF, RLE, Dict")
    } // match

  }

  /** encode into blocks of fixed number of elements
    * return uncompressed size and buffer with compressed data
    */
  def encodeAsLZFBlocks(arr:IntArrayList): (Int, Array[Byte]) = {

    var intsSoFar = 0
    var outSoFar: Int = 0
    logDebug("going to ask for bytes " + (2*LZFSerializer.BLOCK_SIZE))
    var out = new Array[Byte](2*LZFSerializer.BLOCK_SIZE) // extra just in case nothing compresses
    var len = LZFSerializer.BLOCK_SIZE/4 // number of ints to compress at a time
    if (arr.size < LZFSerializer.BLOCK_SIZE/4) len = arr.size

    var runningOffset = 0

    while(intsSoFar < arr.size) {
      logDebug("arr.size, intsSoFar, outSoFar")
      logDebug(List(arr.size, intsSoFar, outSoFar).toString)

      val buffer = ByteBuffer.allocate(4 * len)
      buffer.order(ByteOrder.nativeOrder())
      var i = 0
      while (i < len) {
        buffer.putInt(arr.getInt(i+runningOffset))
        i += 1
      }
      buffer.rewind

      // int len = LZFEncoder.appendEncoded(byte[] input, int inputPtr, int inputLength,
      //                                    byte[] outputBuffer, int outputPtr)

      outSoFar = LZFEncoder.appendEncoded(buffer.array, 0, 4*len, out, outSoFar)
      intsSoFar += len
      runningOffset += 4*len
      if(arr.size - intsSoFar <= (LZFSerializer.BLOCK_SIZE/4)) 
        len = arr.size - intsSoFar
    }

    val encodedArr = new Array[Byte](outSoFar)
    Array.copy(out, 0, encodedArr, 0, outSoFar)

    (outSoFar, encodedArr)
  }
}
