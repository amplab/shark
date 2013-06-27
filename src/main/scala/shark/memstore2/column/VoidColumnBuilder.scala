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


class VoidColumnBuilder extends ColumnBuilder[Void] {

  override def initialize(initialSize: Int) {}

  override def append(o: Object, oi: ObjectInspector) {}

  override def append(v: Void) {}

  override def appendNull() {}

  // Don't collect stats for binary types.
  override def stats: ColumnStats[Void] = null

  override def build: ByteBuffer = {
    val buf = ByteBuffer.allocate(ColumnIterator.COLUMN_TYPE_LENGTH)
    buf.order(ByteOrder.nativeOrder())
    buf.putLong(ColumnIterator.VOID)
    buf.rewind()
    buf
  }
}
