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

package shark.api

import java.util.{List => JList}

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}

import shark.execution.serialization.KryoSerializer

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD


class TableRDD(
    prev: RDD[Any],
    val schema: Array[ColumnDesc],
    @transient oi: ObjectInspector,
    val limit: Int = -1)
  extends RDD[Row](prev) {

  private[shark]
  def this(prev: RDD[Any], schema: JList[FieldSchema], oi: ObjectInspector, limit: Int) {
    this(prev, ColumnDesc.createSchema(schema), oi, limit)
  }

  override def getPartitions = firstParent[Any].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val structOi = initObjectInspector()
    firstParent[Any].iterator(split, context).map { rowData =>
      new Row(rowData, colname2indexMap, structOi)
    }
  }

  /**
   * ObjectInspector is not Java serializable. We serialize it using Kryo and
   * and save it as a byte array. On slave nodes, we deserialize this byte
   * array to obtain the ObjectInspector object.
   */
  private val serializedObjectInspector: Array[Byte] = KryoSerializer.serialize(oi)

  /**
   * Maps the column name to column index.
   */
  private val colname2indexMap: Map[String, Int] =
    collection.immutable.Map() ++ schema.zipWithIndex.map { case(column, index) =>
      (column.name, index)
    }

  /**
   * Initialize object inspector from the serializedObjectInspector.
   */
  private def initObjectInspector(): StructObjectInspector = {
    val oi = KryoSerializer.deserialize[ObjectInspector](serializedObjectInspector)
    oi match {
      case soi: StructObjectInspector => soi
      case _ => throw new Exception("Only basic StructObjectInspector is supposed.")
    }
  }
}
