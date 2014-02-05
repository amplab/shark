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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.hive.ql.metadata.Hive

import org.apache.spark.rdd.RDD

import shark.{SharkContext, SharkEnv}
import shark.memstore2.{CacheType, TablePartitionStats, TablePartition, TablePartitionBuilder}
import shark.util.HiveUtils


class RDDTableFunctions(self: RDD[Seq[_]], classTags: Seq[ClassTag[_]]) {

  def saveAsTable(tableName: String, fields: Seq[String]): Boolean = {
    require(fields.size == this.classTags.size,
      "Number of column names != number of fields in the RDD.")

    // Get a local copy of the classTags so we don't need to serialize this object.
    val classTags = this.classTags

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())

    // Create the RDD object.
    val rdd = self.mapPartitionsWithIndex { case(partitionIndex, iter) =>
      val ois = classTags.map(HiveUtils.getJavaPrimitiveObjectInspector)
      val builder = new TablePartitionBuilder(
        HiveUtils.makeStandardStructObjectInspector(fields, ois),
        1000000,
        shouldCompress = false)

      for (p <- iter) {
        builder.incrementRowCount()
        // TODO: this is not the most efficient code to do the insertion ...
        p.zipWithIndex.foreach { case (v, i) =>
          builder.append(i, v.asInstanceOf[Object], ois(i))
        }
      }

      statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
      Iterator(builder.build())
    }.persist()

    var isSucessfulCreateTable = HiveUtils.createTableInHive(
      tableName, fields, classTags, Hive.get().getConf())

    // Put the table in the metastore. Only proceed if the DDL statement is executed successfully.
    val databaseName = Hive.get(SharkContext.hiveconf).getCurrentDatabase()
    if (isSucessfulCreateTable) {
      // Create an entry in the MemoryMetadataManager.
      val newTable = SharkEnv.memoryMetadataManager.createMemoryTable(
        databaseName, tableName, CacheType.MEMORY)
      try {
        // Force evaluate to put the data in memory.
        rdd.context.runJob(rdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
      } catch {
        case _: Exception => {
          // Intercept the exception thrown by SparkContext#runJob() and handle it silently. The
          // exception message should already be printed to the console by DDLTask#execute().
          HiveUtils.dropTableInHive(tableName)
          // Drop the table entry from MemoryMetadataManager.
          SharkEnv.memoryMetadataManager.removeTable(databaseName, tableName)
          isSucessfulCreateTable = false
        }
      }
      newTable.put(rdd, statsAcc.value.toMap)
    }
    return isSucessfulCreateTable
  }
}
