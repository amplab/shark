/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.util.{Properties, Map => JavaMap}

import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SerializableWritable

import shark.{LogHelper, SharkEnv}
import shark.execution.serialization.KryoSerializer
import shark.memstore2._


private[shark]
class SparkLoadWork(
    val hiveTable: HiveTable,
    val partSpecsOpt: Option[JavaMap[String, String]],
    val isOverwrite: Boolean,
    val pathFilter: Option[PathFilter])
  extends java.io.Serializable

private[shark]
class SparkLoadTask extends HiveTask[SparkLoadWork] with Serializable with LogHelper {

  override def execute(driveContext: DriverContext): Int = {
    logDebug("Executing " + this.getClass.getName)

    val hiveTable = work.hiveTable
    val tableDesc = Utilities.getTableDesc(hiveTable)
    val tableNameSplit = tableDesc.getTableName().split('.')
    val databaseName = tableNameSplit(0)
    val tableName = tableNameSplit(1)
    val oi = hiveTable.getDeserializer().getObjectInspector().asInstanceOf[StructObjectInspector]

    val hadoopReader = new HadoopTableReader(tableDesc, conf)
    val inputRDD = if (hiveTable.isPartitioned) {
      val partition = db.getPartition(hiveTable, work.partSpecsOpt.get, false /* forceCreate */)
      hadoopReader.makeRDDForPartitionedTable(Seq(partition), work.pathFilter)
    } else {
      hadoopReader.makeRDDForTable(hiveTable, work.pathFilter)
    }

    val (tablePartitionRDD, tableStats) = transformInputRdd(
      inputRDD,
      tableDesc.getProperties,
      hadoopReader.broadcastedHiveConf,
      hiveTable.getDeserializer.getObjectInspector().asInstanceOf[StructObjectInspector])

    // TODO(harvey): This part overlaps with, though is slightly cleaner than, execution code in
    //     MemoryStoreSinkOperator.
    // LOAD DATA INPATH behaves like an INSERT, so use a UnionRDD if no OVERWRITE is specified.
    SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
      case Some(table) => {
        var unionWithPreviousStats = false
        table match {
          case memoryTable: MemoryTable => {
            memoryTable.tableRDD = if (work.isOverwrite) {
              tablePartitionRDD
            } else {
              unionWithPreviousStats = true
              RDDUtils.unionAndFlatten(memoryTable.tableRDD, tablePartitionRDD)
            }
          }
          case partitionedTable: PartitionedMemoryTable => {
            val partCols = hiveTable.getPartCols.map(_.getName)
            val partSpecs = work.partSpecsOpt.get
            val partitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpecs)
            partitionedTable.getPartition(partitionKey) match {
              case Some(previousRDD) => {
                partitionedTable.updatePartition(
                  partitionKey, RDDUtils.unionAndFlatten(previousRDD, tablePartitionRDD))
                unionWithPreviousStats = true
              }
              case None => {
                // Either `isOverwrite` is true, or the partition being updated does not currently
                // exist.
                partitionedTable.putPartition(partitionKey, tablePartitionRDD)
              }
            }
          }
        }
        val newStats = if (unionWithPreviousStats) {
          // Union the stats maps.
          SharkEnv.memoryMetadataManager.getStats(databaseName, tableName) match {
            case Some(previousStatsMap) => unionStatsMaps(tableStats, previousStatsMap)
            case _ => tableStats
          }
        } else {
          tableStats
        }
        SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, newStats.toMap)
      }
      case None => {
        throw new Exception("Couldn't find the target table for a LOAD.")
      }
    }

    // Success!
    0
  }

  def transformInputRdd(
      inputRdd: RDD[_],
      tableProps: Properties,
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      oi: StructObjectInspector) = {
    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val serializedOI = KryoSerializer.serialize(oi)
    val transformedRdd = inputRdd.mapPartitionsWithIndex { case (partIndex, partIter) =>
      val serde = new ColumnarSerDe
      serde.initialize(broadcastedHiveConf.value.value, tableProps)
      val oi = KryoSerializer.deserialize[ObjectInspector](serializedOI)
      var builder: Writable = null
      partIter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], oi)
      }
      if (builder == null) {
        // Empty partition.
        statsAcc += Tuple2(partIndex, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      } else {
        statsAcc += Tuple2(partIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      }
    }
    transformedRdd.context.runJob(
      transformedRdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    (transformedRdd, statsAcc.value)
  }

  def unionStatsMaps(
      targetStatsMap: ArrayBuffer[(Int, TablePartitionStats)],
      otherStatsMap: Iterable[(Int, TablePartitionStats)]
    ): ArrayBuffer[(Int, TablePartitionStats)] = {
    val targetStatsMapSize = targetStatsMap.size
    for ((otherIndex, tableStats) <- otherStatsMap) {
      targetStatsMap.append((otherIndex + targetStatsMapSize, tableStats))
    }
    targetStatsMap
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-LOAD-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}
