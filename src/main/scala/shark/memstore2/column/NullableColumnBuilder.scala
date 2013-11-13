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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector


/**
 * Builds a nullable column. The byte buffer of a nullable column contains:
 * - 4 bytes for the null count (number of nulls)
 * - positions for each null, in ascending order
 * - the non-null data (column data type, compression type, data...)
 */
trait NullableColumnBuilder[T] extends ColumnBuilder[T] {

  private var _nulls: ByteBuffer = _

  private var _pos: Int = _
  private var _nullCount: Int = _

  override def initialize(initialSize: Int, cName: String): ByteBuffer = {
    _nulls = ByteBuffer.allocate(1024)
    _nulls.order(ByteOrder.nativeOrder())
    _pos = 0
    _nullCount = 0
    super.initialize(initialSize, cName)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      _nulls = growIfNeeded(_nulls, 4)
      _nulls.putInt(_pos)
      _nullCount += 1
    } else {
      super.append(o, oi)
    }
    _pos += 1
  }

  override def build(): ByteBuffer = {
    val nonNulls = super.build()
    val nullDataLen = _nulls.position()
    _nulls.limit(nullDataLen)
    _nulls.rewind()

    // 4 bytes for null count + null positions + non nulls
    val newBuffer = ByteBuffer.allocate(4 + nullDataLen + nonNulls.limit)
    newBuffer.order(ByteOrder.nativeOrder())
    newBuffer.putInt(_nullCount).put(_nulls).put(nonNulls)
    newBuffer.rewind()
    newBuffer
  }
}
