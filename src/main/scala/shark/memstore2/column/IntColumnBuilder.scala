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

  // Only valid for counts lower than MAX_DICT_UNIQUE_VALUES. Does not get updated after that.
  // Choice made that this is too expensive currently and is not used enough.
  private var uniques = new IntArraySet(DictionarySerializer.MAX_DICT_UNIQUE_VALUES)

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
    if (uniques.size < DictionarySerializer.MAX_DICT_UNIQUE_VALUES) {
      uniques.add(v)
    }
  }

  override def appendNull() {
    _nullBitmap.set(_nonNulls.size + _stats.nullCount)
    _stats.appendNull()
  }

  override def stats = _stats

  def pickCompressionScheme: CompressionScheme.Value = {
    val transitionsRatio = (_stats.transitions).toDouble / _nonNulls.size
    if (transitionsRatio < 0.5) {
      RLE // 1 int for length + 1 int for value - hence 0.5
    } else if (uniques.size < DictionarySerializer.MAX_DICT_UNIQUE_VALUES) {
      Dict
    } else {
      None
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

    val transitionsRatio = (_stats.transitions).toDouble / _nonNulls.size
    logInfo(
      " transitionsRatio=" + transitionsRatio + 
      " transitions=" + _stats.transitions +
      " #values=" + _nonNulls.size)


    scheme match {
      case None => {
        val bufSize = (_nonNulls.size*4) + ColumnIterator.COLUMN_TYPE_LENGTH + sizeOfNullBitmap
        val buf = ByteBuffer.allocate(bufSize)
        logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
          ColumnIterator.COLUMN_TYPE_LENGTH +
          " _nonNulls.size*4 " + _nonNulls.size*4)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + bufSize)

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
        val bufSize = (compressedByteArray.size*1) + 2*4 + ColumnIterator.COLUMN_TYPE_LENGTH +
          sizeOfNullBitmap
        logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
          ColumnIterator.COLUMN_TYPE_LENGTH +
          " 2*4 (numUncompressedBytes, length) " + 2*4 +
          " compressedByteArray.size*1 " + compressedByteArray.size*1)

        val buf = ByteBuffer.allocate(bufSize)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + bufSize)

        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.LZF_INT)

        writeNullBitmap(buf)

        LZFSerializer.writeToBuffer(buf, _nonNulls.size*4, compressedByteArray)

        buf.rewind()
        buf
      }
      case RLE => {
        var rleSs = new RLEStreamingSerializer[Int]( { () => -1 }, { (i, j) => i == j } )
        var i = 0
        while (i < _nonNulls.size) {
          rleSs.encodeSingle(_nonNulls.get(i))
          i += 1
        }

        val rleStrings = rleSs.getCoded
        val vals = rleStrings map (_._2)
        val runs = new IntArrayList(vals.size)
        rleStrings.foreach ( x => runs.add(x._1) )

        val bufSize = 
          4 + //#runs
          runs.size*4 + //runs
          runs.size*4 + //values
          ColumnIterator.COLUMN_TYPE_LENGTH +
          sizeOfNullBitmap

        val buf = ByteBuffer.allocate(bufSize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.RLE_INT)

        logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
          ColumnIterator.COLUMN_TYPE_LENGTH +
          " runs.size*8 " + runs.size*8)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + bufSize)

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
        val dict = new IntDictionary(uniques.toIntArray)
        logInfo("#uniques " + uniques.size)
        val bufSize = (_nonNulls.size*1) + ColumnIterator.COLUMN_TYPE_LENGTH +
        sizeOfNullBitmap + dict.sizeInBytes

        logInfo(
          " ColumnIterator.COLUMN_TYPE_LENGTH " +
            ColumnIterator.COLUMN_TYPE_LENGTH +
            " sizeOfNullBitmap " + sizeOfNullBitmap +
            " dict.sizeInBytes " + dict.sizeInBytes +
            " _nonNulls.size*1 " + _nonNulls.size*1)

        val buf = ByteBuffer.allocate(bufSize)
        logInfo("Allocated ByteBuffer of scheme " + scheme + " size " + bufSize)
        buf.order(ByteOrder.nativeOrder())
        buf.putLong(ColumnIterator.DICT_INT)

        writeNullBitmap(buf)

        IntDictionarySerializer.writeToBuffer(buf, dict)

        var i = 0
        while (i < _nonNulls.size) {
          buf.put(dict.getByte(_nonNulls.get(i)))
          i += 1
        }
        logInfo("Compression ratio is " + (_nonNulls.size.toFloat*4/(bufSize)) + " : 1")
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
