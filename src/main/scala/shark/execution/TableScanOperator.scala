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
import org.apache.spark.rdd.{HadoopRDD, PartitionPruningRDD, RDD, UnionRDD}
import org.apache.spark.SerializableWritable

import shark.{LogHelper, SharkConfVars, SharkEnv, Utils}
import shark.api.QueryExecutionException
import shark.execution.optimization.ColumnPruner
import shark.execution.serialization.{XmlSerializer, JavaSerializer}
import shark.memstore2.{CacheType, TablePartition, TablePartitionStats}
import shark.tachyon.TachyonException


/**
 * The TableScanOperator is used for scanning any type of Shark or Hive table.
 */
class TableScanOperator extends TopOperator[HiveTableScanOperator] with HiveTopOperator {

  @transient var table: Table = _

  // Metadata for Hive-partitions (i.e if the table was created from PARTITION BY). NULL if this
  // table isn't Hive-partitioned. Set in SparkTask::initializeTableScanTableDesc().
  @transient var parts: Array[Object] = _

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


  /**
   * Initialize the hive TableScanOperator. This initialization propagates
   * downstream. When all Hive TableScanOperators are initialized, the entire
   * Hive query plan operators are initialized.
   */
  override def initializeHiveTopOperator() {

    val rowObjectInspector = {
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
        partCols.trim().split("/").foreach{ key =>
          partNames.add(key)
          partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
        }

        // No need to lock this one (see SharkEnv.objectInspectorLock) because
        // this is called on the master only.
        val partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
            partNames, partObjectInspectors)
        val oiList = Arrays.asList(
            tableDeser.getObjectInspector().asInstanceOf[StructObjectInspector],
            partObjectInspector.asInstanceOf[StructObjectInspector])
        // new oi is union of table + partition object inspectors
        ObjectInspectorFactory.getUnionStructObjectInspector(oiList)
      }
    }

    setInputObjectInspector(0, rowObjectInspector)
    super.initializeHiveTopOperator()
  }

  override def initializeOnMaster() {
    // Create a local copy of the HiveConf that will be assigned job properties and, for disk reads,
    // broadcasted to slaves.
    localHConf = new HiveConf(super.hconf)
  }

  override def execute(): RDD[_] = {
    assert(parentOperators.size == 0)
    val tableKey: String = tableDesc.getTableName.split('.')(1)

    // There are three places we can load the table from.
    // 1. Tachyon table
    // 2. Spark heap (block manager), accessed through the Shark MemoryMetadataManager
    // 3. Hive table on HDFS (or other Hadoop storage)
    val cacheMode = CacheType.fromString(
      tableDesc.getProperties().get("shark.cache").asInstanceOf[String])
    if (cacheMode == CacheType.HEAP) {
      // Table should be in Spark heap (block manager).
      val rdd = SharkEnv.memoryMetadataManager.get(tableKey).getOrElse {
        logError("""|Table %s not found in block manager.
                    |Are you trying to access a cached table from a Shark session other than
                    |the one in which it was created?""".stripMargin.format(tableKey))
        throw(new QueryExecutionException("Cached table not found"))
      }
      logInfo("Loading table " + tableKey + " from Spark block manager")
      createPrunedRdd(tableKey, rdd)
    } else if (cacheMode == CacheType.TACHYON) {
      // Table is in Tachyon.
      if (!SharkEnv.tachyonUtil.tableExists(tableKey)) {
        throw new TachyonException("Table " + tableKey + " does not exist in Tachyon")
      }
      logInfo("Loading table " + tableKey + " from Tachyon.")

      var indexToStats: collection.Map[Int, TablePartitionStats] =
        SharkEnv.memoryMetadataManager.getStats(tableKey).getOrElse(null)

      if (indexToStats == null) {
        val statsByteBuffer = SharkEnv.tachyonUtil.getTableMetadata(tableKey)
        indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
          statsByteBuffer.array())
        logInfo("Loading table " + tableKey + " stats from Tachyon.")
        SharkEnv.memoryMetadataManager.putStats(tableKey, indexToStats)
      }
      createPrunedRdd(tableKey, SharkEnv.tachyonUtil.createRDD(tableKey))
    } else {
      // Table is a Hive table on HDFS (or other Hadoop storage).
      super.execute()
    }
  }

  private def createPrunedRdd(tableKey: String, rdd: RDD[_]): RDD[_] = {
    // Stats used for map pruning.
    val indexToStats: collection.Map[Int, TablePartitionStats] =
      SharkEnv.memoryMetadataManager.getStats(tableKey).get

    // Run map pruning if the flag is set, there exists a filter predicate on
    // the input table and we have statistics on the table.
    val columnsUsed = new ColumnPruner(this, table).columnsUsed
    SharkEnv.tachyonUtil.pushDownColumnPruning(rdd, columnsUsed)

    val prunedRdd: RDD[_] =
      if (SharkConfVars.getBoolVar(localHConf, SharkConfVars.MAP_PRUNING) &&
          childOperators(0).isInstanceOf[FilterOperator] &&
          indexToStats.size == rdd.partitions.size) {

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

    prunedRdd.mapPartitions { iter =>
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
   * Create a RDD for a table.
   */
  override def preprocessRdd(rdd: RDD[_]): RDD[_] = {
    // Choose the minimum number of splits. If mapred.map.tasks is set, use that unless
    // it is smaller than what Spark suggests.
    val minSplitsPerRDD = math.max(
      localHConf.getInt("mapred.map.tasks", 1), SharkEnv.sc.defaultMinSplits)

    TableScanOperator.prepareHiveConf(localHConf, hiveOp)
    val broadcastedHiveConf = SharkEnv.sc.broadcast(new SerializableWritable(localHConf))

    if (table.isPartitioned) {
      logDebug("Making %d Hive partitions".format(parts.size))
      makeHivePartitionRDDs(broadcastedHiveConf, minSplitsPerRDD)
    } else {
      makeTableRdd(broadcastedHiveConf, minSplitsPerRDD)
    }
  }

  /**
   * Forward all rows. TableScanOperator doesn't need to do any more processing of values read and
   * preprocessed (i.e., deserialized) from disk.
   *
   * For Hive-partitioned tables, the iterator returns two-element arrays with the elements as
   * (deserialized row, column partition value). For non-partitioned tables, the iterator returns
   * deserialized row Objects.
   */
  override def processPartition(index: Int, iter: Iterator[_]): Iterator[_] = iter

  /**
   * Creates a Hadoop RDD to read data from the target table's data directory. Returns a transformed
   * RDD that contains deserialized rows.
   */
  private def makeTableRdd(
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      minSplits: Int): RDD[_] = {
    val tablePath = table.getPath.toString
    val ifc = table.getInputFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    logDebug("Table input: %s".format(tablePath))

    val hadoopRDD = createHadoopRdd(tablePath, ifc, broadcastedHiveConf, minSplits)
    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHiveConf.value.value
      val deserializer = tableDesc.getDeserializerClass().newInstance()
      deserializer.initialize(hconf, tableDesc.getProperties)

      // Deserialize each Writable to get the row value.
      iter.map { value =>
        value match {
          case v: Writable => deserializer.deserialize(v)
          case _ => throw new RuntimeException("Failed to match " + value.toString)
        }
      }
    }
    return deserializedHadoopRDD
  }

  /**
   * Create an RDD for every partition column specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   */
  private def makeHivePartitionRDDs[T](
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      minSplitsPerRDD: Int
    ): RDD[_] = {
    val partitions = parts
    val hivePartitionRDDs = new Array[RDD[Any]](partitions.size)

    var i = 0
    partitions.foreach { part =>
      val partition = part.asInstanceOf[Partition]
      val partDesc = Utilities.getPartitionDesc(partition)
      val tablePath = partition.getPartitionPath.toString

      val ifc = partition.getInputFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      val hivePartitionRDD = createHadoopRdd(
        tablePath, ifc, broadcastedHiveConf, minSplitsPerRDD)

      val hivePartitionRDDWithColValues = hivePartitionRDD.mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val deserializer = partDesc.getDeserializerClass().newInstance()
        deserializer.initialize(hconf, partDesc.getProperties())

        // Get partition field info
        val partSpec = partDesc.getPartSpec()
        val partProps = partDesc.getProperties()

        val partCols = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
        // Partitioning keys are delimited by "/"
        val partKeys = partCols.trim().split("/")
        // 'partValues[i]' contains the value for the partitioning key at 'partKeys[i]'.
        val partValues = new ArrayList[String]
        partKeys.foreach { key =>
          if (partSpec == null) {
            partValues.add(new String)
          } else {
            partValues.add(new String(partSpec.get(key)))
          }
        }

        val rowWithPartArr = new Array[Object](2)
        // Map each tuple to a row object
        iter.map { value =>
          val deserializedRow = deserializer.deserialize(value) // LazyStruct
          rowWithPartArr.update(0, deserializedRow)
          rowWithPartArr.update(1, partValues)
          rowWithPartArr.asInstanceOf[Object]
        }
      }
      hivePartitionRDDs(i) = hivePartitionRDDWithColValues.asInstanceOf[RDD[Any]]
      i += 1
    }
    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      SharkEnv.sc.makeRDD(Seq[Object]())
    } else {
      new UnionRDD(hivePartitionRDDs(0).context, hivePartitionRDDs)
    }
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each slave.
   */
  private def createHadoopRdd(
      path: String,
      inputFormatClass: Class[InputFormat[Writable, Writable]],
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      minSplits: Int)
    : RDD[Writable] = {
    /*
     * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
     * instantiate a HadoopRDD.
     */
    def initializeLocalJobConfFunc(path: String)(jobConf: JobConf) {
      FileInputFormat.setInputPaths(jobConf, path)
      if (tableDesc != null) {
        Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
      }
      val bufferSize = System.getProperty("spark.buffer.size", "65536")
      jobConf.set("io.file.buffer.size", bufferSize)
    }

    val initializeJobConfFunc = initializeLocalJobConfFunc(path) _

    val rdd = new HadoopRDD(
      SharkEnv.sc,
      broadcastedHiveConf.asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      minSplits)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }
}


object TableScanOperator extends LogHelper {

  /**
   * Add miscellaneous properties to the HiveConf to be used for creating a HadoopRDD. These
   * properties are impractical to add during local JobConf creation in HadoopRDD - for example,
   * filter expressions would require a serialized HiveTableScanOperator.
   */
  private def prepareHiveConf(hiveConf: HiveConf, hiveTableScanOp: HiveTableScanOperator) {
    // Set s3/s3n credentials. Setting them in localJobConf ensures the settings propagate
    // from Spark's master all the way to Spark's slaves.
    var s3varsSet = false
    val s3vars = Seq("fs.s3n.awsAccessKeyId", "fs.s3n.awsSecretAccessKey",
      "fs.s3.awsAccessKeyId", "fs.s3.awsSecretAccessKey").foreach { variableName =>
      if (hiveConf.get(variableName) != null) {
        s3varsSet = true
      }
    }

    // If none of the s3 credentials are set in Hive conf, try use the environmental
    // variables for credentials.
    if (!s3varsSet) {
      Utils.setAwsCredentials(hiveConf)
    }

    TableScanOperator.addFilterExprToConf(hiveConf, hiveTableScanOp)
  }

  /**
   * Add filter expressions and column metadata to the HiveConf This is meant to be called on the
   * master, so that we can avoid serializing the HiveTableScanOperator.
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
    if (filterExprNode == null) return

    val filterText = filterExprNode.getExprString()
    hiveConf.set(TableScanDesc.FILTER_TEXT_CONF_STR, filterText)
    logDebug("Filter text: " + filterText)

    val filterExprNodeSerialized = Utilities.serializeExpression(filterExprNode)
    hiveConf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExprNodeSerialized)
    logDebug("Filter expression: " + filterExprNodeSerialized)
  }
}
