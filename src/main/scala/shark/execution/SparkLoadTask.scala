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

import java.util.{HashMap => JavaHashMap, Properties, Map => JavaMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SerializableWritable
import org.apache.spark.storage.StorageLevel

import shark.{LogHelper, SharkEnv}
import shark.execution.serialization.KryoSerializer
import shark.memstore2._
import shark.util.HiveUtils


private[shark]
class SparkLoadWork(
    val databaseName: String,
    val tableName: String,
    val partSpecOpt: Option[JavaMap[String, String]],
    val commandType: SparkLoadWork.CommandTypes.Type,
    val storageLevel: StorageLevel,
    val pathFilter: Option[PathFilter])
  extends java.io.Serializable

object SparkLoadWork {
  object CommandTypes extends Enumeration {
    type Type = Value
    val OVERWRITE, INSERT, NEW_ENTRY = Value
  }
}

private[shark]
class SparkLoadTask extends HiveTask[SparkLoadWork] with Serializable with LogHelper {

  override def execute(driveContext: DriverContext): Int = {
    logDebug("Executing " + this.getClass.getName)

    val databaseName = work.databaseName
    val tableName = work.tableName
    val hiveTable = Hive.get(conf).getTable(databaseName, tableName)
    val tableDesc = Utilities.getTableDesc(hiveTable)
    val oi = hiveTable.getDeserializer().getObjectInspector().asInstanceOf[StructObjectInspector]

    val sharkTableOpt = SharkEnv.memoryMetadataManager.getTable(databaseName, tableName)
    val hadoopReader = new HadoopTableReader(tableDesc, conf)

    // TODO(harvey): Cleanup. This part overlaps with execution code in MemoryStoreSinkOperator.
    sharkTableOpt match {
      case Some(sharkTable) => {
        var unionWithPreviousStats = false
        sharkTable match {
          case memoryTable: MemoryTable => {
            val serDe = Class.forName(
              memoryTable.diskSerDe).newInstance.asInstanceOf[Deserializer]
            serDe.initialize(conf, tableDesc.getProperties)
            val inputRDD = hadoopReader.makeRDDForTable(
              hiveTable,
              work.pathFilter,
              serDe.getClass)
            val (tablePartitionRDD, tableStats) = transformAndMaterializeInput(
              inputRDD,
              tableDesc.getProperties,
              work.storageLevel,
              hadoopReader.broadcastedHiveConf,
              serDe.getObjectInspector.asInstanceOf[StructObjectInspector])
            memoryTable.tableRDD = work.commandType match {
              case SparkLoadWork.CommandTypes.OVERWRITE => tablePartitionRDD
              case SparkLoadWork.CommandTypes.INSERT => {
                RDDUtils.unionAndFlatten(tablePartitionRDD, memoryTable.tableRDD)
              }
              case SparkLoadWork.CommandTypes.NEW_ENTRY => {
                throw new Exception(
                  "Invalid state: table %s already exists in memory".format(tableName))
              }
            }
            SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
          }
          // TODO(harvey): Multiple partition specs...
          case partitionedTable: PartitionedMemoryTable => {
            val partCols = hiveTable.getPartCols.map(_.getName)
            val partSpecs = work.partSpecOpt.get
            val partitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpecs)
            val partition = db.getPartition(hiveTable, partSpecs, false /* forceCreate */)
            val partSerDe = Class.forName(partitionedTable.getDiskSerDe(partitionKey).getOrElse(
              partitionedTable.diskSerDe)).newInstance.asInstanceOf[Deserializer]
            val partSchema = partition.getSchema
            partSerDe.initialize(conf, partSchema)
            val unionOI = HiveUtils.makeUnionOIForPartitionedTable(partSchema, partSerDe)
            val inputRDD = hadoopReader.makeRDDForPartitionedTable(
              Map(partition -> partSerDe.getClass), work.pathFilter)
            val (tablePartitionRDD, tableStats) = transformAndMaterializeInput(
              inputRDD,
              addPartitionInfoToSerDeProps(partCols, new Properties(partition.getSchema)),
              work.storageLevel,
              hadoopReader.broadcastedHiveConf,
              unionOI)
            partitionedTable.getPartition(partitionKey) match {
              case Some(previousRDD) => {
                work.commandType match {
                  case SparkLoadWork.CommandTypes.OVERWRITE => {
                    partitionedTable.putPartition(partitionKey, tablePartitionRDD)
                  }
                  case SparkLoadWork.CommandTypes.INSERT => {
                    partitionedTable.updatePartition(
                      partitionKey, RDDUtils.unionAndFlatten(tablePartitionRDD, previousRDD))
                    // Union stats for the previous RDD with the new RDD loaded.
                    SharkEnv.memoryMetadataManager.getStats(databaseName, tableName) match {
                      case Some(previousStatsMap) => unionStatsMaps(tableStats, previousStatsMap)
                      case _ => Unit
                    }
                  }
                  case SparkLoadWork.CommandTypes.NEW_ENTRY => {
                    throw new Exception(
                      "Invalid state: table %s already exists in memory".format(tableName))
                  }
                }
              }
              case None => {
                // The partition being updated does not currently exist. Create and set a new
                // partition key entry, and register a shutdown callback in the Shark metastore.
                partitionedTable.putPartition(partitionKey, tablePartitionRDD)
              }
            }
            SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
          }
        }
      }
      case None => {
        // Couldn't find the target table for a LOAD.
        assert(work.commandType == SparkLoadWork.CommandTypes.NEW_ENTRY)

        // Add the table to-be-cached to the Shark metastore.
        val tblProps = hiveTable.getParameters()
        val cacheMode = CacheType.fromString(tblProps.get("shark.cache"))
        val preferredStorageLevel = MemoryMetadataManager.getStorageLevelFromString(
          tblProps.get("shark.cache.storageLevel"))
        val inputRDD = if (hiveTable.isPartitioned) {
          val partition = db.getPartition(hiveTable, work.partSpecOpt.get, false /* forceCreate */)
          hadoopReader.makeRDDForPartitionedTable(Seq(partition))
        } else {
          hadoopReader.makeRDDForTable(hiveTable)
        }
        work.partSpecOpt match {
          case Some(partSpecs) => {
            val hivePartition = Hive.get(conf).getPartition(hiveTable, partSpecs, false /* forceCreate */)
            val partSerDe = hivePartition.getDeserializer
            val partCols = hiveTable.getPartCols.map(_.getName)
            val partSchema = hivePartition.getSchema
            partSerDe.initialize(conf, partSchema)
            val unionOI = HiveUtils.makeUnionOIForPartitionedTable(partSchema, partSerDe)
            // Partitioned table.
            val (tablePartitionRDD, tableStats) = transformAndMaterializeInput(
              inputRDD,
              addPartitionInfoToSerDeProps(partCols, new Properties(hivePartition.getSchema)),
              work.storageLevel,
              hadoopReader.broadcastedHiveConf,
              unionOI)
            val newTable = SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
              databaseName,
              tableName,
              cacheMode,
              preferredStorageLevel,
              unifyView = true,
              tblProps)
            val partitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(
                hiveTable.getPartCols.map(_.getName), partSpecs)
            newTable.putPartition(partitionKey, tablePartitionRDD)
            newTable.setDiskSerDe(partitionKey, hivePartition.getDeserializer.getClass.getName)
            SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
          }
          case None => {
            val serDe = hiveTable.getDeserializer
            val (tablePartitionRDD, tableStats) = transformAndMaterializeInput(
              inputRDD,
              tableDesc.getProperties,
              work.storageLevel,
              hadoopReader.broadcastedHiveConf,
              serDe.getObjectInspector.asInstanceOf[StructObjectInspector])
            // Non-partitioned table.
            val newTable = SharkEnv.memoryMetadataManager.createMemoryTable(
              databaseName, tableName, cacheMode, preferredStorageLevel, unifyView = true)
            newTable.tableRDD = tablePartitionRDD
            // Record what the previous SerDe was.
            newTable.diskSerDe = serDe.getClass.getName
            SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
          }
        }
        // Set the new SerDe to be a ColumnarSerDe, since the data has been cached.
        HiveUtils.alterSerdeInHive(
          tableName,
          work.partSpecOpt,
          classOf[ColumnarSerDe].getName,
          conf)
      }
    }
    // Success!
    0
  }

  def transformAndMaterializeInput(
      inputRdd: RDD[_],
      serDeProps: Properties,
      storageLevel: StorageLevel,
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      oi: StructObjectInspector) = {
    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val serializedOI = KryoSerializer.serialize(oi)
    val transformedRdd = inputRdd.mapPartitionsWithIndex { case (partIndex, partIter) =>
      val serde = new ColumnarSerDe
      serde.initialize(broadcastedHiveConf.value.value, serDeProps)
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
    transformedRdd.persist(storageLevel)
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

  def addPartitionInfoToSerDeProps(
    partCols: Seq[String],
    serDeProps: Properties): Properties = {
    // Delimited by ","
    var columnNameProperty: String = serDeProps.getProperty(Constants.LIST_COLUMNS)
    // NULL if column types are missing. By default, the SerDeParameters initialized by the
    // ColumnarSerDe will treat all columns as having string types.
    // Delimited by ":"
    var columnTypeProperty: String = serDeProps.getProperty(Constants.LIST_COLUMN_TYPES)

    for (partColName <- partCols) {
      columnNameProperty += "," + partColName
    }
    if (columnTypeProperty != null) {
      for (partColName <- partCols) {
        columnTypeProperty += ":" + Constants.STRING_TYPE_NAME
      }
    }
    serDeProps.setProperty(Constants.LIST_COLUMNS, columnNameProperty)
    serDeProps.setProperty(Constants.LIST_COLUMN_TYPES, columnTypeProperty)
    serDeProps
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-LOAD-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}
