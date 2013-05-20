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

import scala.collection.immutable.BitSet

import shark.memstore2.column.ColumnIterator
import shark.memstore2.column.ColumnIteratorFactory


/**
 * An iterator for a partition of data. Each element returns a ColumnarStruct
 * that can be read by a ColumnarStructObjectInspector.
 *
 * @param numRows: total number of rows in this partition.
 * @param columnIterators: iterators for all columns.
 @ @param columnUsed: an optional bitmap indicating whether a column is used.
 */
class TablePartitionIterator(
    val numRows: Long,
    val columnIterators: Array[ColumnIterator],
    val columnUsed: BitSet = null)
  extends Iterator[ColumnarStruct] {

  private val _struct = new ColumnarStruct(columnIterators)

  private var _position: Long = 0

  def hasNext(): Boolean = _position < numRows

  def next(): ColumnarStruct = {
    _position += 1
    var i = 0
    while (i < _columnIteratorsToAdvance.size) {
      _columnIteratorsToAdvance(i).next
      i += 1
    }
    _struct
  }

  // Track the list of columns we need to call next on.
  private val _columnIteratorsToAdvance: Array[ColumnIterator] = {
    if (columnUsed == null) {
      columnIterators
    } else {
      columnUsed.map(colId => columnIterators(colId)).toArray
    }
  }
}
