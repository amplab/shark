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

import java.util.{ArrayList, Arrays}

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator}
import org.apache.hadoop.hive.ql.exec.{MapSplitPruning, Utilities}
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.hadoop.hive.ql.plan.{PlanUtils, PartitionDesc, TableDesc, TableScanDesc}
import org.apache.hadoop.hive.serde.Constants
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Writable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, PartitionPruningRDD, RDD, UnionRDD}
import org.apache.spark.SerializableWritable

import shark.{LogHelper, SharkConfVars, SharkEnv, Utils}
import shark.api.QueryExecutionException
import shark.execution.optimization.ColumnPruner
import shark.execution.serialization.{XmlSerializer, JavaSerializer}
import shark.memstore2.{CacheType, MemoryMetadataManager, TablePartition, TablePartitionStats}
import shark.tachyon.TachyonException


/**
 * The TableScanOperator is used for scanning any type of Shark or Hive table.
 */
class TableScanOperator extends TopOperator[TableScanDesc] {

  @transient var table: Table = _
  @transient var hiveOp: HiveTableScanOperator = _

  // Metadata for Hive-partitions (i.e if the table was created from PARTITION BY). NULL if this
  // table isn't Hive-partitioned. Set in SparkTask::initializeTableScanTableDesc().
  @transient var parts: Array[Partition] = _

  // For convenience, a local copy of the HiveConf for this task.
  @transient var localHConf: HiveConf = _

  // PartitionDescs are used during planning in Hive. This reference to a single PartitionDesc
  // is used to initialize partition ObjectInspectors.
  // If the table is not Hive-partitioned, then 'firstConfPartDesc' won't be used. The value is not
  // NULL, but rather a reference to a "dummy" PartitionDesc, in which only the PartitionDesc's
  // 'table' is not NULL.
  // Set in SparkTask::initializeTableScanTableDesc().
  @BeanProperty var firstConfPartDesc: PartitionDesc  = _

  @BeanProperty var tableDesc: TableDesc = _


  override def initializeOnMaster() {
    // Create a local copy of the HiveConf that will be assigned job properties and, for disk reads,
    // broadcasted to slaves.
    localHConf = new HiveConf(super.hconf)
  }

  override def outputObjectInspector() = {
    if (parts == null) {
      val serializer = tableDesc.getDeserializerClass().newInstance()
      serializer.initialize(hconf, tableDesc.getProperties)
      serializer.getObjectInspector()
    } else {
      val partProps = firstConfPartDesc.getProperties()
      val tableDeser = firstConfPartDesc.getDeserializerClass().newInstance()
      tableDeser.initialize(hconf, partProps)
      val partCols = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      val partNames = new ArrayList[String]
      val partObjectInspectors = new ArrayList[ObjectInspector]
      partCols.trim().split("/").foreach { key =>
        partNames.add(key)
        partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      }

      val partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        partNames, partObjectInspectors)
      val oiList = Arrays.asList(
        tableDeser.getObjectInspector().asInstanceOf[StructObjectInspector],
        partObjectInspector.asInstanceOf[StructObjectInspector])
      // new oi is union of table + partition object inspectors
      ObjectInspectorFactory.getUnionStructObjectInspector(oiList)
    }
  }

  override def execute(): RDD[_] = {
    assert(parentOperators.size == 0)

    val tableNameSplit = tableDesc.getTableName.split('.') // Split from 'databaseName.tableName'
    val databaseName = tableNameSplit(0)
    val tableName = tableNameSplit(1)
    // There are three places we can load the table from.
    // 1. Tachyon table
    // 2. Spark heap (block manager), accessed through the Shark MemoryMetadataManager
    // 3. Hive table on HDFS (or other Hadoop storage)
    val cacheMode = CacheType.fromString(
      tableDesc.getProperties().get("shark.cache").asInstanceOf[String])
    // TODO(harvey): Pruning Hive-partitioned, cached tables isn't supported yet.
    if (cacheMode == CacheType.HEAP) {
      // Table should be in Spark heap (block manager).
      if (!SharkEnv.memoryMetadataManager.containsTable(databaseName, tableName)) {
        logError("""|Table %s.%s not found in block manager.
                    |Are you trying to access a cached table from a Shark session other than the one
                    |in which it was created?""".stripMargin.format(databaseName, tableName))
        throw new QueryExecutionException("Cached table not found")
      }
      if (SharkEnv.memoryMetadataManager.isHivePartitioned(databaseName, tableName)) {
        // Get the union of RDDs repesenting the selected Hive partition(s).
        return makeCachedPartitionRDD(databaseName, tableName, parts)
      } else {
        val table = SharkEnv.memoryMetadataManager.getMemoryTable(databaseName, tableName).get
        logInfo("Loading table %s.%s from Spark block manager".format(databaseName, tableName))
        return createPrunedRdd(databaseName, tableName, table.tableRDD)
      }
    } else if (cacheMode == CacheType.TACHYON) {
      // Table is in Tachyon.
      val tableKey = SharkEnv.makeTachyonTableKey(databaseName, tableName)
      if (!SharkEnv.tachyonUtil.tableExists(tableKey)) {
        throw new TachyonException("Table " + tableKey + " does not exist in Tachyon")
      }
      logInfo("Loading table " + tableKey + " from Tachyon.")

      var indexToStats: collection.Map[Int, TablePartitionStats] =
        SharkEnv.memoryMetadataManager.getStats(databaseName, tableName).getOrElse(null)

      if (indexToStats == null) {
        val statsByteBuffer = SharkEnv.tachyonUtil.getTableMetadata(tableKey)
        indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
          statsByteBuffer.array())
        logInfo("Loading table " + tableKey + " stats from Tachyon.")
        SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, indexToStats)
      }
      return createPrunedRdd(databaseName, tableName, SharkEnv.tachyonUtil.createRDD(tableName))
    } else {
      // Table is a Hive table on HDFS (or other Hadoop storage).
      return makeRDDFromHadoop()
    }
  }

  private def createPrunedRdd(databaseName: String, tableName: String, rdd: RDD[_]): RDD[_] = {
    // Stats used for map pruning.
    val indexToStats: collection.Map[Int, TablePartitionStats] =
      SharkEnv.memoryMetadataManager.getStats(databaseName, tableName).get

    // Run map pruning if the flag is set, there exists a filter predicate on
    // the input table and we have statistics on the table.
    val columnsUsed = new ColumnPruner(this, table).columnsUsed
    SharkEnv.tachyonUtil.pushDownColumnPruning(rdd, columnsUsed)

    val shouldPrune = (SharkConfVars.getBoolVar(localHConf, SharkConfVars.MAP_PRUNING) &&
                       childOperators(0).isInstanceOf[FilterOperator] &&
                       indexToStats.size == rdd.partitions.size)

    val prunedRdd: RDD[_] = if (shouldPrune) {
      val startTime = System.currentTimeMillis
      val printPruneDebug = SharkConfVars.getBoolVar(
        localHConf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)

      // Must initialize the condition evaluator in FilterOperator to get the
      // udfs and object inspectors set.
      val filterOp = childOperators(0).asInstanceOf[FilterOperator]
      filterOp.initializeOnSlave()

      def prunePartitionFunc(index: Int): Boolean = {
        if (printPruneDebug) {
          logInfo("\nPartition " + index + "\n" + indexToStats(index))
        }
        // Only test for pruning if we have stats on the column.
        val partitionStats = indexToStats(index)
        if (partitionStats != null && partitionStats.stats != null) {
          MapSplitPruning.test(partitionStats, filterOp.conditionEvaluator)
        } else {
          true
        }
      }

      // Do the pruning.
      val prunedRdd = PartitionPruningRDD.create(rdd, prunePartitionFunc)
      val timeTaken = System.currentTimeMillis - startTime
      logInfo("Map pruning %d partitions into %s partitions took %d ms".format(
          rdd.partitions.size, prunedRdd.partitions.size, timeTaken))
      prunedRdd
    } else {
      rdd
    }

    return prunedRdd.mapPartitions { iter =>
      if (iter.hasNext) {
        val tablePartition = iter.next.asInstanceOf[TablePartition]
        tablePartition.prunedIterator(columnsUsed)
        //tablePartition.iterator
      } else {
        Iterator()
      }
    }
  }

  /**
   * Create an RDD for a table stored in Hadoop.
   */
  def makeRDDFromHadoop(): RDD[_] = {
    // Try to have the InputFormats filter predicates.
    TableScanOperator.addFilterExprToConf(localHConf, hiveOp)

    val hadoopReader = new HadoopTableReader(tableDesc, localHConf)
    if (table.isPartitioned) {
      logDebug("Making %d Hive partitions".format(parts.size))
      // The returned RDD contains arrays of size two with the elements as
      // (deserialized row, column partition value).
      return hadoopReader.makeRDDForTablePartitions(parts)
    } else {
      // The returned RDD contains deserialized row Objects.
      return hadoopReader.makeRDDForTable(table)
    }
  }

  /**
   * Fetch an RDD from the Shark metastore using each partition key given, and return a union of all
   * the fetched RDDs.
   *
   * @param tableKey Name of the partitioned table.
   * @param partitions A collection of Hive-partition metadata, such as partition columns and
   *     partition key specifications.
   */
  private def makeCachedPartitionRDD(
      databaseName: String,
      tableName: String,
      partitions: Array[Partition]): RDD[_] = {
    val hivePartitionRDDs = partitions.map { partition =>
      val partDesc = Utilities.getPartitionDesc(partition)
      // Get partition field info
      val partSpec = partDesc.getPartSpec()
      val partProps = partDesc.getProperties()

      val partColsDelimited = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      // Partitioning columns are delimited by "/"
      val partCols = partColsDelimited.trim().split("/").toSeq
      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      val partitionKeyStr = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpec)
      val hivePartitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
        databaseName, tableName).get
      val hivePartitionRDD = hivePartitionedTable.getPartition(partitionKeyStr)

      hivePartitionRDD.get.mapPartitions { iter =>
        if (iter.hasNext) {
          // Map each tuple to a row object
          val rowWithPartArr = new Array[Object](2)
          val tablePartition = iter.next.asInstanceOf[TablePartition]
          tablePartition.iterator.map { value =>
            rowWithPartArr.update(0, value.asInstanceOf[Object])
            rowWithPartArr.update(1, partValues)
            rowWithPartArr.asInstanceOf[Object]
          }
        } else {
         Iterator()
        }
      }
    }
    if (hivePartitionRDDs.size > 0) {
      new UnionRDD(hivePartitionRDDs.head.context, hivePartitionRDDs)
    } else {
      new EmptyRDD[Object](SharkEnv.sc)
    }
  }

  // All RDD processing is done in execute().
  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("TableScanOperator.processPartition()")
}


object TableScanOperator extends LogHelper {

  /**
   * Add filter expressions and column metadata to the HiveConf. This is meant to be called on the
   * master - it's impractical to add filters during slave-local JobConf creation in HadoopRDD,
   * since we would have to serialize the HiveTableScanOperator.
   */
  private def addFilterExprToConf(hiveConf: HiveConf, hiveTableScanOp: HiveTableScanOperator) {
    val tableScanDesc = hiveTableScanOp.getConf()
    if (tableScanDesc == null) return

    val rowSchema = hiveTableScanOp.getSchema
    if (rowSchema != null) {
      // Add column names to the HiveConf.
      var columnNames = new StringBuilder()
      for (columnInfo <- rowSchema.getSignature()) {
        if (columnNames.length() > 0) {
          columnNames.append(",")
        }
        columnNames.append(columnInfo.getInternalName())
      }
      val columnNamesString = columnNames.toString();
      hiveConf.set(Constants.LIST_COLUMNS, columnNamesString);

      // Add column types to the HiveConf.
      var columnTypes = new StringBuilder()
      for (columnInfo <- rowSchema.getSignature()) {
        if (columnTypes.length() > 0) {
          columnTypes.append(",")
        }
        columnTypes.append(columnInfo.getType().getTypeName())
      }
      val columnTypesString = columnTypes.toString
      hiveConf.set(Constants.LIST_COLUMN_TYPES, columnTypesString)
    }

    // Push down predicate filters.
    val filterExprNode = tableScanDesc.getFilterExpr()
    if (filterExprNode != null) {
      val filterText = filterExprNode.getExprString()
      hiveConf.set(TableScanDesc.FILTER_TEXT_CONF_STR, filterText)
      logDebug("Filter text: " + filterText)

      val filterExprNodeSerialized = Utilities.serializeExpression(filterExprNode)
      hiveConf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExprNodeSerialized)
      logDebug("Filter expression: " + filterExprNodeSerialized)
    }
  }
}
