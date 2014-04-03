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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.BitSet
import shark.memstore2.column.ColumnIterator


/**
 * TablePartition contains a whole partition of data in columnar format. It
 * simply contains a list of columns and their meta data. It should be built
 * using a TablePartitionBuilder.
 */
class TablePartition(private var _numRows: Long, private var _columns: Array[ByteBuffer])
  extends Externalizable {

  // Empty constructor for Externalizable
  def this() {
    this(0, null)
  }

  def this(columns: Array[ByteBuffer]) {
    this(columns(0).getLong(), columns.tail)
  }

  def numRows: Long = _numRows

  def columns: Array[ByteBuffer] = _columns

  /** We store our per-partition metadata in a fake "column 0" for off-heap storage. */
  def toOffHeap: Array[ByteBuffer] = {
    val buffers = new Array[ByteBuffer](1 + _columns.size)
    buffers(0) = metadata
    System.arraycopy(_columns, 0, buffers, 1, _columns.size)
    buffers
  }

  def metadata: ByteBuffer = {
    val buffer = ByteBuffer.allocate(8)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putLong(_numRows)
    buffer.rewind()
    buffer
  }

  /**
   * Return an iterator for the partition.
   */
  def iterator: TablePartitionIterator = {
    val columnIterators: Array[ColumnIterator] = _columns.map { case buffer: ByteBuffer =>
      val iter = ColumnIterator.newIterator(buffer)
      iter
    }
    new TablePartitionIterator(_numRows, columnIterators)
  }

  def prunedIterator(columnsUsed: BitSet) = {
    val columnIterators: Array[ColumnIterator] = _columns.map {
      case buffer: ByteBuffer =>
        ColumnIterator.newIterator(buffer)
      case _ =>
        // The buffer might be null if it is pruned in off-heap storage.
        null
    }
    new TablePartitionIterator(_numRows, columnIterators, columnsUsed)
  }

  override def readExternal(in: ObjectInput) {
    _numRows = in.readLong()
    val numColumns = in.readInt()
    _columns = Array.fill[ByteBuffer](numColumns) {
      val columnLen = in.readInt()
      val buf = ByteBuffer.allocate(columnLen)
      in.readFully(buf.array(), 0, columnLen)
      buf
    }
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeLong(numRows)
    out.writeInt(columns.length)
    for (column <- columns) {
      val buf = column.duplicate()
      buf.rewind()
      // If the ByteBuffer is backed by a byte array, just write the byte array out.
      // Otherwise, write each byte one by one.
      if (buf.hasArray()) {
        val byteArray = buf.array()
        out.writeInt(byteArray.length)
        out.write(byteArray, 0, byteArray.length)
      } else {
        out.writeInt(buf.remaining())
        while (buf.hasRemaining()) {
          out.write(buf.get())
        }
      }
    }
  }
}
