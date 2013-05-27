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
  private var _arr: IntArrayList = null
  private val MAX_UNIQUE_VALUES = 256 // 2 ** 8 - storable in 8 bits or 1 Byte
  private var _uniques: collection.mutable.Set[Int] = new HashSet()
  private var COMPRESSED = false
  def isCompressed = COMPRESSED

  override def initialize(initialSize: Int) {
    _arr = new IntArrayList(initialSize)
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
    _arr.add(v)
    _stats.append(v)
    _uniques += v
  }

  override def appendNull() {
    _nullBitmap.set(_arr.size)
    _arr.add(0)
    _stats.appendNull()
  }

  override def stats = _stats

  override def build: ByteBuffer = {
    logDebug("IntColumnBuilder " + _uniques.size + " unique values")
    if (_uniques.size >= MAX_UNIQUE_VALUES) {
      COMPRESSED = false
      val minbufsize = (_arr.size*4) + ColumnIterator.COLUMN_TYPE_LENGTH + sizeOfNullBitmap
      val buf = ByteBuffer.allocate(minbufsize)
      logInfo("Allocated ByteBuffer of size " + minbufsize)

      buf.order(ByteOrder.nativeOrder())
      buf.putLong(ColumnIterator.INT)

      writeNullBitmap(buf)

      var i = 0
      while (i < _arr.size) {
        buf.putInt(_arr.get(i))
        i += 1
      }
      buf.rewind()
      buf
    }
    else {
      COMPRESSED = true
      val dict = new Dictionary(_uniques.toList)
      val minbufsize = (_arr.size*1) + ColumnIterator.COLUMN_TYPE_LENGTH +
        sizeOfNullBitmap + (dict.sizeinbits/8)

      val buf = ByteBuffer.allocate(minbufsize)
      logInfo("Allocated ByteBuffer of size " + minbufsize)

      buf.order(ByteOrder.nativeOrder())
      buf.putLong(ColumnIterator.DICT_INT)

      writeNullBitmap(buf)

      DictionarySerializer.writeToBuffer(buf, dict)

      var i = 0
      while (i < _arr.size) {
        buf.put(dict.getByte(_arr.get(i)))
        i += 1
      }
      logInfo("Compression ratio is " + 
        (_arr.size.toFloat*4/(minbufsize)) +
        " : 1")
      buf.rewind()
      buf
    }
  }
}
