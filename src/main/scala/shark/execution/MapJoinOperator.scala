package shark.execution

import java.util.{ArrayList, HashMap => JHashMap, List => JList}

import org.apache.hadoop.io.BytesWritable

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, JoinUtil}
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator.{HashTableSinkObjectCtx => MapJoinObjectCtx}
import org.apache.hadoop.hive.ql.exec.MapJoinMetaData
import org.apache.hadoop.hive.ql.exec.{MapJoinOperator => HiveMapJoinOperator}
import org.apache.hadoop.hive.ql.exec.persistence.{AbstractMapJoinKey, MapJoinDoubleKeys}
import org.apache.hadoop.hive.ql.exec.persistence.{MapJoinObjectKey, MapJoinSingleKey}
import org.apache.hadoop.hive.ql.exec.persistence.{MapJoinRowContainer, MapJoinObjectValue}
import org.apache.hadoop.hive.ql.plan.MapJoinDesc
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.SerDe

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import spark.broadcast.Broadcast
import spark.RDD
import shark.SharkEnvSlave

object MapJoinOperator {
  type MapJoinHashTable = JHashMap[AbstractMapJoinKey, MapJoinObjectValue]
}

/**
 * A join operator optimized for joining a large table with a number of small
 * tables that fit in memory. The join can be performed as a map only job that
 * avoids an expensive shuffle process.
 *
 * Different from Hive, we don't spill the hash tables to disk. If the "small"
 * tables are too big to fit in memory, the normal join should be used anyway.
 */
class MapJoinOperator extends CommonJoinOperator[MapJoinDesc, HiveMapJoinOperator] {

  @BeanProperty var posBigTable: Int = _
  @BeanProperty var bigTableAlias: Int = _
  @BeanProperty var bigTableAliasByte: java.lang.Byte = _

  @transient var joinKeys: JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]] = _
  @transient var joinKeysObjectInspectors: JHashMap[java.lang.Byte, JList[ObjectInspector]] = _

  @transient val metadataKeyTag = -1
  @transient var joinValues: JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]] = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    posBigTable = conf.getPosBigTable()
    bigTableAlias = order(posBigTable).toInt
    bigTableAliasByte = bigTableAlias.toByte

    // Also call initialize on slave since we want the joinKeys and joinVals to
    // be initialized so we can use them in combineMultipleRdds(). This also puts
    // serialization info for keys in MapJoinMetaData.
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    joinKeys = new JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]]
    JoinUtil.populateJoinKeyValue(
      joinKeys, conf.getKeys(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    // A bit confusing but getObjectInspectorsFromEvaluators also initializes
    // the evaluators.
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(
      joinKeys, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)

    // Put serialization metadata for keys in MapJoinMetaData.
    setKeyMetaData()
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    logInfo("%d small tables to map join a large table (%d)".format(rdds.size - 1, posBigTable))

    val op1 = OperatorSerializationWrapper(this)

    initializeOnSlave()

    // Build hash tables for the small tables.
    val hashtables = rdds.zipWithIndex.filter(_._2 != bigTableAlias).map { case ((_, rdd), pos) =>

      logInfo("Creating hash table for input %d".format(pos))

      // First compute the keys and values of the small RDDs on slaves.
      // We need to do this before collecting the RDD because the RDD might
      // contain lazy structs that cannot be properly collected directly.
      val posByte = pos.toByte

      // Put serialization metadata for values in master's MapJoinMetaData.
      // Needed to deserialize values in collect().
      setValueMetaData(posByte)

      // Create a local reference for the serialized arrays, otherwise the
      // following mapParititons will fail because it tries to include the
      // outer closure, which references "this".
      val op = op1
      val rddForHash: RDD[(AbstractMapJoinKey, MapJoinObjectValue)] =
        rdd.mapPartitions { partition =>
          op.initializeOnSlave()
          // Put serialization metadata for values in slave's MapJoinMetaData.
          // Needed to serialize values in collect().
          op.setValueMetaData(posByte)
          op.computeJoinKeyValuesOnPartition(partition, posByte)
        }

      // Collect the RDD and build a hash table.
      val startCollect = System.currentTimeMillis()
      val wrappedRows: Array[(AbstractMapJoinKey, MapJoinObjectValue)] = rddForHash.collect()
      val collectTime = System.currentTimeMillis() - startCollect

      // Build the hash table.
      val startHash = System.currentTimeMillis()
      val hashTable = new MapJoinOperator.MapJoinHashTable
      wrappedRows.foreach { case (wrappedKey, wrappedValue) =>
        var mapEntry = hashTable.get(wrappedKey)
        if (mapEntry == null) {
          val container = new MapJoinRowContainer[Array[Object]]
          mapEntry = new MapJoinObjectValue(posByte, container)
          hashTable.put(wrappedKey, mapEntry)
          container.setList(wrappedValue.getObj.getList())
        } else {
          wrappedValue.getObj.getList().foreach(mapEntry.getObj().add)
        }
      }
      val hashTime = System.currentTimeMillis() - startHash
      logInfo("Input %d (%d rows) took %d ms to collect and %s ms to build hash table.".format(
        pos, hashTable.size, collectTime, hashTime))

      setValueMetaData(posByte)

      (pos, hashTable)
    }.toMap

    val fetcher = new MapJoinHashTablesBroadcast(hashtables)

    val op = op1
    rdds(bigTableAlias)._2.mapPartitions { partition =>
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave()
      val newPart = op.joinOnPartition(partition, fetcher.get)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      newPart
    }
  }

  def computeJoinKeyValuesOnPartition[T](iter: Iterator[T], posByte: Byte)
  : Iterator[(AbstractMapJoinKey, MapJoinObjectValue)] = {
    // MapJoinObjectValue contains a MapJoinRowContainer, which contains a list of
    // rows to be joined.
    var valueMap = new JHashMap[AbstractMapJoinKey, MapJoinObjectValue]
    iter.foreach { row =>
      val key = JoinUtil.computeMapJoinKeys(
        row,
        joinKeys.get(posByte),
        joinKeysObjectInspectors.get(posByte))
      val value = JoinUtil.computeMapJoinValues(
        row,
        joinVals.get(posByte),
        joinValuesObjectInspectors.get(posByte),
        joinFilters.get(posByte),
        joinFilterObjectInspectors.get(posByte),
        noOuterJoin)
      // If we've seen the key before, just add it to the row container wrapped by
      // corresponding MapJoinObjectValue.
      val objValue = valueMap.get(key)
      if (objValue == null) {
        val rowContainer = new MapJoinRowContainer[Array[Object]]
        rowContainer.add(value)
        valueMap.put(key, new MapJoinObjectValue(posByte, rowContainer))
      } else {
        val rowContainer = objValue.getObj
        rowContainer.add(value)
      }
    }
    valueMap.iterator
  }

  def setKeyMetaData() {
    MapJoinMetaData.clear()

    val keyTableDesc = conf.getKeyTblDesc()
    val keySerializer = keyTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    keySerializer.initialize(null, keyTableDesc.getProperties())

    val standardOI = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorUtils.getStandardObjectInspector(
        keySerializer.getObjectInspector(), ObjectInspectorCopyOption.WRITABLE)
    }

    // MapJoinMetaData is a static object. Wrap it around synchronized to be thread safe.
    this.synchronized {
      MapJoinMetaData.put(Integer.valueOf(metadataKeyTag), new MapJoinObjectCtx(
        standardOI, keySerializer, keyTableDesc, hconf))
    }
  }

  def setValueMetaData(pos: Byte) {
    val valueTableDesc = conf.getValueFilteredTblDescs().get(pos)
    val valueSerDe = valueTableDesc.getDeserializerClass().newInstance.asInstanceOf[SerDe]

    valueSerDe.initialize(null, valueTableDesc.getProperties())

    val newFields = joinValuesStandardObjectInspectors.get(pos)
    val length = newFields.size()
    val newNames = new java.util.ArrayList[String](length)
    for (i <- 0 until length) newNames.add(new String("tmp_" + i))

    val standardOI = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(newNames, newFields)
    }

    // MapJoinMetaData is a static object. Wrap it around synchronized to be thread safe.
    this.synchronized {
      MapJoinMetaData.put(Integer.valueOf(pos), new MapJoinObjectCtx(
        standardOI, valueSerDe, valueTableDesc, hconf))
    }
  }

  /**
   * Stream through the large table and process the join using the hash tables.
   * Note that this is a specialized processPartition that accepts an extra
   * parameter for the hash tables (built from the small tables).
   */
  def joinOnPartition[T](iter: Iterator[T], hashtables: Map[Int, MapJoinOperator.MapJoinHashTable])
  : Iterator[_] = {

    val joinKeyEval = joinKeys.get(bigTableAlias.toByte)
    val joinValueEval = joinVals.get(bigTableAlias.toByte)
    val bufs = new Array[Seq[Array[Object]]](numTables)
    val nullSafes = conf.getNullSafes()

    val cp = new CartesianProduct[Array[Object]](numTables)

    val jointRows: Iterator[Array[Array[Object]]] = iter.flatMap { row =>
      // Build the join key and value for the row in the large table.
      val key: AbstractMapJoinKey = JoinUtil.computeMapJoinKeys(
        row,
        joinKeyEval,
        joinKeysObjectInspectors.get(bigTableAliasByte))
      val value: Array[Object] = JoinUtil.computeMapJoinValues(
        row,
        joinValueEval,
        joinValuesObjectInspectors.get(bigTableAliasByte),
        joinFilters.get(bigTableAliasByte),
        joinFilterObjectInspectors.get(bigTableAliasByte),
        noOuterJoin)

      if (nullCheck && key.hasAnyNulls(nullSafes)) {
        val bufsNull = Array.fill[Seq[Array[Object]]](numTables)(Seq())
        bufsNull(bigTableAlias) = Seq(value)
        cp.product(bufsNull.asInstanceOf[Array[Seq[Array[Object]]]], joinConditions)
      } else {
        // Build the join bufs.
        var i = 0
        while ( i < numTables) {
          if (i == bigTableAlias) {
            bufs(i) = Seq[Array[Object]](value)
          } else {
            val smallTableValues: MapJoinObjectValue = hashtables.getOrElse(i, null).get(key)
            bufs(i) =
              if (smallTableValues == null) Seq[Array[Object]]()
              else smallTableValues.getObj().getList().asInstanceOf[ArrayList[Array[Object]]]
          }
          i += 1
        }
        cp.product(bufs.asInstanceOf[Array[Seq[Array[Object]]]], joinConditions)
      }
    }

    val rowSize = joinVals.values.map(_.size).sum
    val rowToReturn = new Array[Object](rowSize)

    // For each row, combine the tuples from multiple tables into a single tuple.
    jointRows.map { row: Array[Array[Object]] =>
      var tupleIndex = 0
      var fieldIndex = 0
      row.foreach { tuple =>
        val stop = fieldIndex + joinVals(tupleIndex.toByte).size
        var fieldInTuple = 0
        if (tuple == null) {
          // For outer joins, it is possible to see nulls.
          while (fieldIndex < stop) {
            rowToReturn(fieldIndex) = null
            fieldInTuple += 1
            fieldIndex += 1
          }
        } else {
          while (fieldIndex < stop) {
            rowToReturn(fieldIndex) = tuple.asInstanceOf[Array[Object]](fieldInTuple)
            fieldInTuple += 1
            fieldIndex += 1
          }
        }
        tupleIndex += 1
      }
      rowToReturn
    }
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] =
    throw new UnsupportedOperationException("MapJoinOperator.processPartition()")
}
