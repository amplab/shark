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
import scala.reflect.BeanProperty
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator}
import org.apache.hadoop.hive.ql.exec.{MapSplitPruning, Utilities}
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.hadoop.hive.ql.plan.{PlanUtils, PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Writable

import org.apache.spark.rdd.{PartitionPruningRDD, RDD, UnionRDD}

import shark.{SharkConfVars, SharkEnv, Utils}
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

  // PartitionDescs are used during planning in Hive. This reference to a single PartitionDesc
  // is used to initialize partition ObjectInspectors.
  // If the table is not Hive-partitioned, then 'firstConfPartDesc' won't be used. The value is not
  // NULL, but rather a reference to a "dummy" PartitionDesc, in which only the PartitionDesc's
  // 'table' is not NULL.
  // Set in SparkTask::initializeTableScanTableDesc().
  @BeanProperty var firstConfPartDesc: PartitionDesc  = _

  @BeanProperty var tableDesc: TableDesc = _
  @BeanProperty var localHconf: HiveConf = _

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
    localHconf = super.hconf
  }

  override def execute(): RDD[_] = {
    assert(parentOperators.size == 0)
    val tableKey: String = tableDesc.getTableName.split('.')(1)

    // There are three places we can load the table from.
    // 1. Tachyon table
    // 2. Spark heap (block manager)
    // 3. Hive table on HDFS (or other Hadoop storage)

    val cacheMode = CacheType.fromString(
      tableDesc.getProperties().get("shark.cache").asInstanceOf[String])
    if (cacheMode == CacheType.heap) {
      // Table should be in Spark heap (block manager).
      val rdd = SharkEnv.memoryMetadataManager.get(tableKey).getOrElse {
        logError("""|Table %s not found in block manager.
                    |Are you trying to access a cached table from a Shark session other than
                    |the one in which it was created?""".stripMargin.format(tableKey))
        throw(new QueryExecutionException("Cached table not found"))
      }
      logInfo("Loading table " + tableKey + " from Spark block manager")
      createPrunedRdd(tableKey, rdd)
    } else if (cacheMode == CacheType.tachyon) {
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
      if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING) &&
          childOperators(0).isInstanceOf[FilterOperator] &&
          indexToStats.size == rdd.partitions.size) {

        val startTime = System.currentTimeMillis
        val printPruneDebug = SharkConfVars.getBoolVar(
          localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)

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
   * Create a RDD representing the table (with or without partitions).
   */
  override def preprocessRdd(rdd: RDD[_]): RDD[_] = {
    if (table.isPartitioned) {
      logInfo("Making %d Hive partitions".format(parts.size))
      makePartitionRDD(rdd)
    } else {
      val tablePath = table.getPath.toString
      val ifc = table.getInputFormatClass
          .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      logInfo("Table input: %s".format(tablePath))
      createHadoopRdd(tablePath, ifc)
    }
  }

  override def processPartition(index: Int, iter: Iterator[_]): Iterator[_] = {
    val deserializer = tableDesc.getDeserializerClass().newInstance()
    deserializer.initialize(localHconf, tableDesc.getProperties)
    iter.map { value =>
      value match {
        case rowWithPart: Array[Object] => rowWithPart
        case v: Writable => deserializer.deserialize(v)
        case _ => throw new RuntimeException("Failed to match " + value.toString)
      }
    }
  }

  /**
   * Create an RDD for every partition column specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   */
  private def makePartitionRDD[T](rdd: RDD[T]): RDD[_] = {
    val partitions = parts
    val rdds = new Array[RDD[Any]](partitions.size)

    var i = 0
    partitions.foreach { part =>
      val partition = part.asInstanceOf[Partition]
      val partDesc = Utilities.getPartitionDesc(partition)
      val tablePath = partition.getPartitionPath.toString

      val ifc = partition.getInputFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      val parts = createHadoopRdd(tablePath, ifc)

      val serializedHconf = XmlSerializer.serialize(localHconf, localHconf)
      val partRDD = parts.mapPartitions { iter =>
        val hconf = XmlSerializer.deserialize(serializedHconf).asInstanceOf[HiveConf]
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
      rdds(i) = partRDD.asInstanceOf[RDD[Any]]
      i += 1
    }
    // Even if we don't use any partitions, we still need an empty RDD
    if (rdds.size == 0) {
      SharkEnv.sc.makeRDD(Seq[Object]())
    } else {
      new UnionRDD(rdds(0).context, rdds)
    }
  }

  private def createHadoopRdd(path: String, ifc: Class[InputFormat[Writable, Writable]])
  : RDD[Writable] = {
    val conf = new JobConf(localHconf)
    if (tableDesc != null) {
      Utilities.copyTableJobPropertiesToConf(tableDesc, conf)
    }
    new HiveInputFormat() {
      def doPushFilters() {
        pushFilters(conf, hiveOp)
      }
    }.doPushFilters()
    FileInputFormat.setInputPaths(conf, path)
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    conf.set("io.file.buffer.size", bufferSize)

    // Set s3/s3n credentials. Setting them in conf ensures the settings propagate
    // from Spark's master all the way to Spark's slaves.
    var s3varsSet = false
    val s3vars = Seq("fs.s3n.awsAccessKeyId", "fs.s3n.awsSecretAccessKey",
      "fs.s3.awsAccessKeyId", "fs.s3.awsSecretAccessKey").foreach { variableName =>
      if (localHconf.get(variableName) != null) {
        s3varsSet = true
        conf.set(variableName, localHconf.get(variableName))
      }
    }

    // If none of the s3 credentials are set in Hive conf, try use the environmental
    // variables for credentials.
    if (!s3varsSet) {
      Utils.setAwsCredentials(conf)
    }

    // Choose the minimum number of splits. If mapred.map.tasks is set, use that unless
    // it is smaller than what Spark suggests.
    val minSplits = math.max(localHconf.getInt("mapred.map.tasks", 1), SharkEnv.sc.defaultMinSplits)
    val rdd = SharkEnv.sc.hadoopRDD(conf, ifc, classOf[Writable], classOf[Writable], minSplits)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }
}
