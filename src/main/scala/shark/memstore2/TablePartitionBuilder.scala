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

import java.io.{DataInput, DataOutput}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.io.Writable

import shark.memstore2.column.ColumnBuilder


/**
 * Used to build a TablePartition. This is used in the serializer to convert a
 * partition of data into columnar format and to generate a TablePartition.
 */
class TablePartitionBuilder(
    ois: Seq[ObjectInspector],
    initialColumnSize: Int,
    shouldCompress: Boolean)
  extends Writable {

  def this(oi: StructObjectInspector, initialColumnSize: Int, shouldCompress: Boolean = true) = {
    this(oi.getAllStructFieldRefs.map(_.getFieldObjectInspector), initialColumnSize, shouldCompress)
  }

  private var numRows: Long = 0

  private val columnBuilders: Array[ColumnBuilder[_]] = ois.map { oi =>
    val columnBuilder = ColumnBuilder.create(oi, shouldCompress)
    columnBuilder.initialize(initialColumnSize)
    columnBuilder
  }.toArray

  def incrementRowCount() {
    numRows += 1
  }

  def append(columnIndex: Int, o: Object, oi: ObjectInspector) {
    columnBuilders(columnIndex).append(o, oi)
  }

  def stats: TablePartitionStats = new TablePartitionStats(columnBuilders.map(_.stats), numRows)

  def build(): TablePartition = new TablePartition(numRows, columnBuilders.map(_.build()))

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) {}
  override def readFields(in: DataInput) {}
}
