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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import it.unimi.dsi.fastutil.ints.IntArrayList

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector

import collection.mutable.{Set, HashSet}

class IntColumnBuilder extends ColumnBuilder[Int]{

  // logger problems - rmeove before commit
  private def logInfo(msg: String) = { println("INFO " + msg) }
  private def logDebug(msg: String) = { println("DEBUG " + msg) }

  private var _stats: ColumnStats.IntColumnStats = new ColumnStats.IntColumnStats
  private var _nonNulls: IntArrayList = null
  private val MAX_UNIQUE_VALUES = 256 // 2 ** 8 - storable in 8 bits or 1 Byte

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
  }

  override def appendNull() {
    _nullBitmap.set(_nonNulls.size + _stats.nullCount)
    _stats.appendNull()
  }

  override def stats = _stats

  def pickCompressionScheme: String = {
    // RLE choice logic - use RLE if the
    // selectivity is < 20% &&
    // ratio of transitions < 50% (run+value is 2 ints instead of 1)
    val selectivity = (_stats.uniques.size).toDouble / _nonNulls.size
    val transitionsRatio = (_stats.transitions).toDouble / _nonNulls.size
    logInfo("uniques=" + _stats.uniques.size + " selectivity=" + selectivity + 
      " transitionsRatio=" + transitionsRatio + 
      " transitions=" + _stats.transitions +
      " #values=" + _nonNulls.size)

    if(selectivity < 0.2) {
      if (transitionsRatio < 0.5) return "RLE"
      else return "dict"
    } else {
      return "none"
    }
  }


  var scheme = "auto"

  override def build: ByteBuffer = {

    scheme = System.getenv("TEST_SHARK_INT_COLUMN_COMPRESSION_SCHEME")
    if(scheme == null || scheme == "auto") scheme = pickCompressionScheme
    // choices are none, auto, RLE, dict

    scheme.toUpperCase match {
      case "NONE" => {
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
      case "RLE" => {
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
      case "DICT" => {
        val dict = new IntDictionary
        logInfo("#uniques " + _stats.uniques.size)
        dict.initialize(_stats.uniques.toList)
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
        logInfo("Compression ratio is " +
          (_nonNulls.size.toFloat*4/(minbufsize)) +
          " : 1")
        buf.rewind()
        buf
      }
      case _ => throw new IllegalArgumentException(
        "scheme must be one of auto, none, RLE, dict")
    } // match

  }
}
