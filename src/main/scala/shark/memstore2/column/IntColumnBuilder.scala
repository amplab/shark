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

import shark.LogHelper

class IntColumnBuilder extends ColumnBuilder[Int] with LogHelper{

  private var _stats: ColumnStats.IntColumnStats = null
  private var _nonNulls: IntArrayList = null
  private val MAX_UNIQUE_VALUES = 256 // 2 ** 8 - storable in 8 bits or 1 Byte
  private var _uniques: collection.mutable.Set[Int] = new HashSet()
  // compressionPossible is true by default but becomes false after there are
  // more than MAX_UNIQUE_VALUES.
  var compressionPossible = true

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
    // compressionPossible is used to short-circuit adding elements to _uniques
    // if there are too many elements
    if (compressionPossible) {
      _uniques += v
      if (_uniques.size >= MAX_UNIQUE_VALUES) 
        compressionPossible = false
    }
  }

  override def appendNull() {
    _nullBitmap.set(_nonNulls.size + _stats.nullCount)
    _stats.appendNull()
  }

  override def stats = _stats

  override def build: ByteBuffer = {
    logDebug("IntColumnBuilder " + _uniques.size + " unique values")
    if (!compressionPossible) {
      val minbufsize = (_nonNulls.size*4) + ColumnIterator.COLUMN_TYPE_LENGTH + sizeOfNullBitmap
      val buf = ByteBuffer.allocate(minbufsize)
      logDebug("sizeOfNullBitmap " + sizeOfNullBitmap +
        " ColumnIterator.COLUMN_TYPE_LENGTH " +
        ColumnIterator.COLUMN_TYPE_LENGTH +
        " _nonNulls.size*4 " + _nonNulls.size*4)
      logInfo("Allocated ByteBuffer of size " + minbufsize)


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
    } else { // compressionPossible = true
      val dict = new IntDictionary
      logInfo("#uniques " + _uniques.size) 
      dict.initialize(_uniques.toList)
      val minbufsize = (_nonNulls.size*1) + ColumnIterator.COLUMN_TYPE_LENGTH +
        sizeOfNullBitmap + (dict.sizeInBits/8)

      logInfo(
        " ColumnIterator.COLUMN_TYPE_LENGTH " +
        ColumnIterator.COLUMN_TYPE_LENGTH +
        " sizeOfNullBitmap " + sizeOfNullBitmap +
        " dict.sizeinbits/8 " + (dict.sizeInBits/8) +
        " _nonNulls.size*1 " + _nonNulls.size*1)

      val buf = ByteBuffer.allocate(minbufsize)
      logInfo("Allocated ByteBuffer (compressed) of size " + minbufsize)

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
  }
}
