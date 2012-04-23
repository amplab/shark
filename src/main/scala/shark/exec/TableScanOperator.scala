package shark.exec

import java.util.{ArrayList, Arrays}

import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Writable

import scala.reflect.BeanProperty

import shark.{CacheKey, SharkEnv, SharkUtilities}
import spark.RDD


class TableScanOperator extends TopOperator[HiveTableScanOperator]
with HiveTopOperator with Serializable {

  @transient var table: Table = _

  @BeanProperty var parts: Array[Object] = _
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
      if (parts == null ) {
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
    val tableKey = new CacheKey(tableDesc.getTableName.split('.')(1))
    SharkEnv.cache.get(tableKey) match {
      case Some(rdd) => {
        logInfo("Loading table from cache " + tableKey)
        Operator.executeProcessPartition(this, rdd)
      }
      case None => super.execute()
    }
  }

  /**
   * Create a RDD representing the table (with or without partitions).
   */
  override def preprocessRdd[T](rdd: RDD[T]): RDD[_] = {
    if (table.isPartitioned) {
      logInfo("Making %d Hive partitions".format(parts.size))
      makePartitionRDD(rdd)
    } else {
      val tablePath = table.getPath.toString
      val ifc = table.getInputFormatClass
          .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      logInfo("Table input: %s".format(tablePath))
      SharkEnv.sc.hadoopFile(
        tablePath, ifc, classOf[Writable], classOf[Writable]).map(_._2)
    }
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    val deserializer = tableDesc.getDeserializerClass().newInstance()
    deserializer.initialize(localHconf, tableDesc.getProperties)
    iter.map { value => 
      value match {
        case rowWithPart: Array[Object] => rowWithPart
        case v: Writable => deserializer.deserialize(v)
      }
    }
  }
  
  def makePartitionRDD[T](rdd: RDD[T]): RDD[_] = {
    var rowsRDD: RDD[Object] = null
    val partitions = parts

    partitions.foreach { part =>
      val partition = part.asInstanceOf[Partition]
      val partDesc = Utilities.getPartitionDesc(partition)
      val tablePath = partition.getPartitionPath.toString

      val ifc = partition.getInputFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

      val parts = SharkEnv.sc.hadoopFile(
        tablePath, ifc, classOf[Writable], classOf[Writable]).map(_._2)

      val serializedHconf = SharkUtilities.xmlSerialize(localHconf)
      val partRDD = parts.mapPartitions { iter =>
        // Map each tuple to a row object
        val hconf = SharkUtilities.xmlDeserialize(serializedHconf).asInstanceOf[HiveConf]
        val deserializer = partDesc.getDeserializerClass().newInstance()
        deserializer.initialize(hconf, partDesc.getProperties())

        // Get partition field info
        val partSpec = partDesc.getPartSpec()
        val partProps = partDesc.getProperties()

        val partCols = partProps.getProperty(
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS)
        val partKeys = partCols.trim().split("/")
        val partValues = new ArrayList[String]
        partKeys.foreach { key =>
          if (partSpec == null) {
            partValues.add(new String)
          } else {
            partValues.add(new String(partSpec.get(key)))
          }
        }

        val rowWithPartArr = new Array[Object](2)
        iter.map { value =>
          val deserializedRow = deserializer.deserialize(value) // LazyStruct
          rowWithPartArr.update(0, deserializedRow)
          rowWithPartArr.update(1, partValues)
          rowWithPartArr.asInstanceOf[Object]
        }
      }

      rowsRDD = rowsRDD match  {
        case null => partRDD
        case _ => rowsRDD.union(partRDD)
      }
    }
    // Even if we don't use any partitions, we still need an empty RDD
    if (rowsRDD == null) {
      SharkEnv.sc.makeRDD(Seq[Object]())
    } else {
      rowsRDD
    }
  }

}
