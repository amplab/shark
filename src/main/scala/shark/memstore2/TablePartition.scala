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

package shark.memstore2

import java.nio.ByteBuffer
import java.nio.ByteOrder

import shark.memstore2.column.ColumnIterator


/**
 * TablePartition contains a whole partition of data in columnar format. It
 * simply contains a list of columns and their meta data. It should be built
 * using a TablePartitionBuilder.
 */
class TablePartition(val numRows: Long, val columns: Array[ByteBuffer]) {

  def this(columns: Array[ByteBuffer]) {
    this(columns(0).getLong(), columns.tail)
  }

  def toTachyon: Array[ByteBuffer] = {
    val buffers = new Array[ByteBuffer](1 + columns.size)
    buffers(0) = metadata
    System.arraycopy(columns, 0, buffers, 1, columns.size)
    buffers
  }

  def metadata: ByteBuffer = {
    val buffer = ByteBuffer.allocate(8)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putLong(numRows)
    buffer.rewind()
    buffer
  }

  /**
   * Return an iterator for the partition.
   */
  def iterator: TablePartitionIterator = {
    val columnIterators: Array[ColumnIterator] = columns.map { case buffer: ByteBuffer =>
      val columnType = buffer.getInt()
      val iter = ColumnIterator.getIteratorClass(columnType).newInstance
      iter.initialize(buffer)
      iter
    }
    new TablePartitionIterator(numRows, columnIterators)
  }
}
