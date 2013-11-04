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
import shark.tachyon.TachyonTableWriter


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

  // Storage level to use for the RDD created and materialized by this sink operator.
  @BeanProperty var storageLevel: StorageLevel = _

  // For CTAS, this is the name of the table that is created. For INSERTS, this is the name of
  // the table that is modified.
  @BeanProperty var tableName: String = _

  // The Hive metastore DB that the `tableName` table belongs to.
  @BeanProperty var databaseName: String = _

  // Used only for commands that target Hive partitions. The partition key is a set of unique values
  // for the the table's partitioning columns and identifies the partition (represented by an RDD)
  // that will be created or modified by the INSERT command being handled.
  @BeanProperty var hivePartitionKey: String = _

  // The memory storage used to store the output RDD - e.g., CacheType.HEAP refers to Spark's
  // block manager.
  @transient var cacheMode: CacheType.CacheType = _

  // Whether to compose a UnionRDD from the output RDD and a previous RDD. For example, for an
  // INSERT INTO <tableName> command, the previous RDD will contain the contents of the 'tableName'.
  @transient var useUnionRDD: Boolean = _

  // The number of columns in the schema for the table corresponding to 'tableName'. Used only
  // to create a TachyonTableWriter, if Tachyon is used.
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

    val tachyonWriter: TachyonTableWriter =
      if (cacheMode == CacheType.TACHYON) {
        // Use an additional row to store metadata (e.g. number of rows in each partition).
        SharkEnv.tachyonUtil.createTableWriter(tableName, numColumns + 1)
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

      if (builder != null) {
        statsAcc += Tuple2(part, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build())
      } else {
        // Empty partition.
        statsAcc += Tuple2(part, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      }
    }

    val isHivePartitioned = SharkEnv.memoryMetadataManager.isHivePartitioned(
      databaseName, tableName)

    // If true, a UnionRDD will be used to combine the RDD that contains the query output with the
    // previous RDD, which is fetched using 'tableName' or - if the table is Hive-partitioned - a
    // ('tableName', 'hivePartitionKey') pair.
    var hasPreviousRDDForUnion = false

    if (tachyonWriter != null) {
      // Put the table in Tachyon.
      op.logInfo("Putting RDD for %s.%s in Tachyon".format(databaseName, tableName))
      tachyonWriter.createTable(ByteBuffer.allocate(0))
      outputRDD = outputRDD.mapPartitionsWithIndex { case(part, iter) =>
        val partition = iter.next()
        partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
          tachyonWriter.writeColumnPartition(column, part, buf)
        }
        Iterator(partition)
      }
      // Force evaluate so the data gets put into Tachyon.
      outputRDD.context.runJob(
        outputRDD, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    } else {
      // Put the table in Spark block manager.
      op.logInfo("Putting %sRDD for %s.%s in Spark block manager, %s %s %s %s".format(
        if (useUnionRDD) "Union" else "",
        databaseName,
        tableName,
        if (storageLevel.deserialized) "deserialized" else "serialized",
        if (storageLevel.useMemory) "in memory" else "",
        if (storageLevel.useMemory && storageLevel.useDisk) "and" else "",
        if (storageLevel.useDisk) "on disk" else ""))

      outputRDD.persist(storageLevel)

      val queryOutputRDD = outputRDD
      if (useUnionRDD) {
        // Handle an INSERT INTO command.
        val previousRDDOpt: Option[RDD[TablePartition]] = if (isHivePartitioned) {
          val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
            databaseName, tableName).get
          partitionedTable.getPartition(hivePartitionKey)
        } else {
          SharkEnv.memoryMetadataManager.getMemoryTable(databaseName, tableName).map(_.tableRDD)
        }
        outputRDD = previousRDDOpt match {
          case Some(previousRDD) => {
            // If the RDD for a table or Hive-partition has already been created, then take a union
            // of the current data and the SELECT output.
            hasPreviousRDDForUnion = true
            RDDUtils.unionAndFlatten(queryOutputRDD, previousRDD)
          }
          // This is an INSERT into a new Hive-partition.
          case None => queryOutputRDD
        }
      }
      // Run a job on the RDD that contains the query output to force the data into the memory
      // store. The statistics will also be collected by 'statsAcc' during job execution.
      queryOutputRDD.context.runJob(
        queryOutputRDD, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    }

    if (isHivePartitioned) {
      val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
        databaseName, tableName).get
      outputRDD.setName("%s.%s(%s)".format(databaseName, tableName, hivePartitionKey))
      if (useUnionRDD && hasPreviousRDDForUnion) {
        // An RDD for the Hive partition already exists, so update its metadata entry in
        // 'partitionedTable'.
        assert(outputRDD.isInstanceOf[UnionRDD[_]])
        partitionedTable.updatePartition(hivePartitionKey, outputRDD)
      } else {
        // This is a new Hive-partition. Add a new metadata entry in 'partitionedTable'.
        partitionedTable.putPartition(hivePartitionKey, outputRDD)
      }
    } else {
      outputRDD.setName(tableName)
      // Create a new MemoryTable entry if one doesn't exist (i.e., this operator is for a CTAS).
      val memoryTable = SharkEnv.memoryMetadataManager.getMemoryTable(databaseName, tableName)
        .getOrElse(SharkEnv.memoryMetadataManager.createMemoryTable(
          databaseName, tableName, cacheMode, storageLevel))
      memoryTable.tableRDD = outputRDD
    }

    // TODO(harvey): Get this to work for Hive-partitioned tables. It should be a simple
    //     'tableName' + 'hivePartitionKey' concatentation. Though whether stats should belong in
    //     memstore2.Table should be considered...
    val columnStats = if (useUnionRDD && hasPreviousRDDForUnion) {
      // Combine stats for the two RDDs that were combined into UnionRDD.
      val numPartitions = statsAcc.value.toMap.size
      val currentStats = statsAcc.value
      SharkEnv.memoryMetadataManager.getStats(databaseName, tableName) match {
        case Some(otherIndexToStats) => {
          for ((otherIndex, tableStats) <- otherIndexToStats) {
            currentStats.append((otherIndex + numPartitions, tableStats))
          }
        }
        case _ => Unit
      }
      currentStats.toMap
    } else {
      statsAcc.value.toMap
    }

    // Get the column statistics back to the cache manager.
    SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, columnStats)

    if (tachyonWriter != null) {
      tachyonWriter.updateMetadata(ByteBuffer.wrap(JavaSerializer.serialize(columnStats)))
    }

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      columnStats.foreach { case(index, tableStats) =>
        println("Partition " + index + " " + tableStats.toString)
      }
    }

    return outputRDD
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}
