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

package shark.execution

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import org.apache.hadoop.io.Writable

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import shark.{SharkConfVars, SharkEnv}
import shark.execution.serialization.{OperatorSerializationWrapper, JavaSerializer}
import shark.memstore2._


/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class MemoryStoreSinkOperator extends TerminalOperator {

  // The initial capacity for ArrayLists used to construct the columnar storage. If -1,
  // the ColumnarSerde will obtain the partition size from a Configuration during execution
  // initialization (see ColumnarSerde#initialize()).
  @BeanProperty var partitionSize: Int = _

  // If true, columnar storage will use compression.
  @BeanProperty var shouldCompress: Boolean = _

  // For CTAS, this is the name of the table that is created. For INSERTS, this is the name of*
  // the table that is modified.
  @BeanProperty var tableName: String = _

  // The Hive metastore DB that the `tableName` table belongs to.
  @BeanProperty var databaseName: String = _

  // Used only for commands that target Hive partitions. The partition key is a set of unique values
  // for the the table's partitioning columns and identifies the partition (represented by an RDD)
  // that will be created or modified by the INSERT command being handled.
  @BeanProperty var hivePartitionKeyOpt: Option[String] = _

  // The memory storage used to store the output RDD - e.g., CacheType.HEAP refers to Spark's
  // block manager.
  @transient var cacheMode: CacheType.CacheType = _

  // Whether to compose a UnionRDD from the output RDD and a previous RDD. For example, for an
  // INSERT INTO <tableName> command, the previous RDD will contain the contents of the 'tableName'.
  @transient var isInsertInto: Boolean = _

  // The number of columns in the schema for the table corresponding to 'tableName'. Used only
  // to create an OffHeapTableWriter, if off-heap storage is used.
  @transient var numColumns: Int = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    partitionSize = SharkConfVars.getIntVar(localHconf, SharkConfVars.COLUMN_BUILDER_PARTITION_SIZE)
    shouldCompress = SharkConfVars.getBoolVar(localHconf, SharkConfVars.COLUMNAR_COMPRESSION)
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    localHconf.setInt(SharkConfVars.COLUMN_BUILDER_PARTITION_SIZE.varname, partitionSize)
    localHconf.setBoolean(SharkConfVars.COLUMNAR_COMPRESSION.varname, shouldCompress)
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val op = OperatorSerializationWrapper(this)
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)

    val offHeapWriter: OffHeapTableWriter =
      if (cacheMode == CacheType.OFFHEAP) {
        val offHeapClient = OffHeapStorageClient.client
        if (!isInsertInto && offHeapClient.tablePartitionExists(tableKey, hivePartitionKeyOpt)) {
          // For INSERT OVERWRITE, delete the old table or Hive partition directory, if it exists.
          offHeapClient.dropTablePartition(tableKey, hivePartitionKeyOpt)
        }
        // Use an additional row to store metadata (e.g. number of rows in each partition).
        offHeapClient.createTablePartitionWriter(tableKey, hivePartitionKeyOpt, numColumns + 1)
      } else {
        null
      }

    // Put all rows of the table into a set of TablePartition's. Each partition contains
    // only one TablePartition object.
    var outputRDD: RDD[TablePartition] = inputRdd.mapPartitionsWithIndex { case (part, iter) =>
      op.initializeOnSlave()
      val serde = new ColumnarSerDe
      serde.initialize(op.localHconf, op.localHiveOp.getConf.getTableInfo.getProperties)

      // Serialize each row into the builder object.
      // ColumnarSerDe will return a TablePartitionBuilder.
      var builder: Writable = null
      iter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], op.objectInspector)
      }

      if (builder == null) {
        // Empty partition.
        statsAcc += Tuple2(part, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      } else {
        statsAcc += Tuple2(part, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      }
    }

    if (offHeapWriter != null) {
      // Put the table in off-heap storage.
      op.logInfo("Putting RDD for %s.%s in off-heap storage".format(databaseName, tableName))
      offHeapWriter.createTable()
      outputRDD = outputRDD.mapPartitionsWithIndex { case(part, iter) =>
        val partition = iter.next()
        partition.toOffHeap.zipWithIndex.foreach { case(buf, column) =>
          offHeapWriter.writeColumnPartition(column, part, buf)
        }
        Iterator(partition)
      }
      // Force evaluate so the data gets put into off-heap storage.
      outputRDD.context.runJob(
        outputRDD, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    } else {
      // Run a job on the RDD that contains the query output to force the data into the memory
      // store. The statistics will also be collected by 'statsAcc' during job execution.
      if (cacheMode == CacheType.MEMORY) {
        outputRDD.persist(StorageLevel.MEMORY_AND_DISK)
      } else if (cacheMode == CacheType.MEMORY_ONLY) {
        outputRDD.persist(StorageLevel.MEMORY_ONLY)
      }
      outputRDD.context.runJob(
        outputRDD, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    }

    // Put the table in Spark block manager or off-heap storage.
    op.logInfo("Putting %sRDD for %s.%s in %s store".format(
      if (isInsertInto) "Union" else "",
      databaseName,
      tableName,
      if (cacheMode == CacheType.NONE) "disk" else cacheMode.toString))

    val tableStats =
      if (cacheMode == CacheType.OFFHEAP) {
        offHeapWriter.setStats(statsAcc.value.toMap)
        statsAcc.value.toMap
      } else {
        val isHivePartitioned = SharkEnv.memoryMetadataManager.isHivePartitioned(
          databaseName, tableName)
        if (isHivePartitioned) {
          val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
            databaseName, tableName).get
          val hivePartitionKey = hivePartitionKeyOpt.get
          outputRDD.setName("%s.%s(%s)".format(databaseName, tableName, hivePartitionKey))
          if (isInsertInto) {
            // An RDD for the Hive partition already exists, so update its metadata entry in
            // 'partitionedTable'.
            partitionedTable.updatePartition(hivePartitionKey, outputRDD, statsAcc.value)
          } else {
            // This is a new Hive-partition. Add a new metadata entry in 'partitionedTable'.
            partitionedTable.putPartition(hivePartitionKey, outputRDD, statsAcc.value.toMap)
          }
          // Stats should be updated at this point.
          partitionedTable.getStats(hivePartitionKey).get
        } else {
          outputRDD.setName(tableName)
          // Create a new MemoryTable entry if one doesn't exist (i.e., this operator is for a CTAS).
          val memoryTable = SharkEnv.memoryMetadataManager.getMemoryTable(databaseName, tableName)
            .getOrElse(SharkEnv.memoryMetadataManager.createMemoryTable(
              databaseName, tableName, cacheMode))
          if (isInsertInto) {
            // Ok, a off-heap table should manage stats for each rdd, and never union the maps.
            memoryTable.update(outputRDD, statsAcc.value)
          } else {
            memoryTable.put(outputRDD, statsAcc.value.toMap)
          }
          memoryTable.getStats.get
        }
      }

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      tableStats.foreach { case(index, tablePartitionStats) =>
        println("Partition " + index + " " + tablePartitionStats.toString)
      }
    }

    return outputRDD
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}
