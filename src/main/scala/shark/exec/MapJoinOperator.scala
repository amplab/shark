package shark.exec

import java.util.{HashMap => JavaHashMap, List => JavaList}

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, JoinUtil}
import org.apache.hadoop.hive.ql.exec.{MapJoinOperator => HiveMapJoinOperator}
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey
import org.apache.hadoop.hive.ql.plan.MapJoinDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.BytesWritable

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.SharkEnv
import shark.collections.Conversions._
import spark.RDD
import spark.broadcast.Broadcast


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

  @transient var joinKeys: JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]] = _
  @transient var joinKeysObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    posBigTable = conf.getPosBigTable()
    bigTableAlias = order(posBigTable).toInt
    bigTableAliasByte = bigTableAlias.toByte

    // Also call initialize on slave since we want the joinKeys and joinVals to
    // be initialized so we can use them in combineMultipleRdds().
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    joinKeys = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    JoinUtil.populateJoinKeyValue(
      joinKeys, conf.getKeys(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    // A bit confusing but getObjectInspectorsFromEvaluators also initializes
    // the evaluators.
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(
      joinKeys, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    logInfo("%d small tables to map join a large table (%d)".format(rdds.size - 1, posBigTable))

    val op1 = OperatorSerializationWrapper(this)

    // Build hash tables for the small tables.
    val hashtables = rdds.zipWithIndex.filter(_._2 != bigTableAlias).map { case ((_, rdd), pos) =>

      logInfo("Creating hash table for input %d".format(pos))

      // First compute the keys and values of the small RDDs on slaves.
      // We need to do this before collecting the RDD because the RDD might
      // contain lazy structs that cannot be properly collected directly.
      val posByte = pos.toByte

      // Create a local reference for the serialized arrays, otherwise the
      // following mapParititons will fail because it tries to include the
      // outer closure, which references "this".
      val op = op1
      val rddForHash: RDD[(AbstractMapJoinKey, Array[java.lang.Object])] =
        rdd.mapPartitions { partition =>
          op.initializeOnSlave()
          op.computeJoinKeyValuesOnPartition(partition, posByte)
        }

      // Collect the RDD and build a hash table.
      val startCollect = System.currentTimeMillis()
      val rows: Array[(AbstractMapJoinKey, Array[java.lang.Object])] = rddForHash.collect()
      val collectTime = System.currentTimeMillis() - startCollect

      val startHash = System.currentTimeMillis()
      val hashtable = rows.toSeq.groupByKey()
      val hashTime = System.currentTimeMillis() - startHash
      logInfo("Input %d (%d rows) took %d ms to collect and %s ms to build hash table.".format(
        pos, rows.size, collectTime, hashTime))

      (pos, hashtable)
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
  : Iterator[(AbstractMapJoinKey, Array[java.lang.Object])] = {
    iter.map { row =>
      val key = JoinUtil.computeMapJoinKeys(
        row,
        joinKeys(posByte),
        joinKeysObjectInspectors(posByte))
      val value = JoinUtil.computeMapJoinValues(
        row,
        joinVals(posByte),
        joinValuesObjectInspectors(posByte),
        joinFilters(posByte),
        joinFilterObjectInspectors(posByte),
        noOuterJoin)
      (key, value)
    }
  }

  /**
   * Stream through the large table and process the join using the hash tables.
   * Note that this is a specialized processPartition that accepts an extra
   * parameter for the hash tables (built from the small tables).
   */
  def joinOnPartition[T](iter: Iterator[T], hashtables: Map[Int, MapJoinHashTable])
  : Iterator[_] = {
    
    val joinKeyEval = joinKeys(bigTableAlias.toByte)
    val joinValueEval = joinVals(bigTableAlias.toByte)
    val bufs = new Array[Seq[Array[java.lang.Object]]](numTables)

    val jointRows = iter.flatMap { row =>
      // Build the join key and value for the row in the large table.
      val key: AbstractMapJoinKey = JoinUtil.computeMapJoinKeys(
        row,
        joinKeyEval,
        joinKeysObjectInspectors(bigTableAliasByte))
      val value: Array[java.lang.Object] = JoinUtil.computeMapJoinValues(
        row,
        joinValueEval,
        joinValuesObjectInspectors(bigTableAliasByte),
        joinFilters.get(bigTableAliasByte),
        joinFilterObjectInspectors(bigTableAliasByte),
        noOuterJoin)

      // Build the join bufs.
      var i = 0
      while ( i < numTables) {
        if (i == bigTableAlias) {
          bufs(i) = Seq(value)
        } else {
          val hashtable: MapJoinHashTable = hashtables.getOrElse(i, null)
          bufs(i) = hashtable.getOrElse(key, Seq())
        }
        i += 1
      }

      CommonJoinOperator.cartesianProduct(bufs.asInstanceOf[Array[Seq[Any]]], joinConditions)
    }

    val rowSize = joinVals.values.map(_.size).sum
    val rowToReturn = new Array[Object](rowSize)

    // For each row, combine the tuples from multiple tables into a single tuple.
    jointRows.map { row =>
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
            rowToReturn(fieldIndex) = tuple.asInstanceOf[Array[java.lang.Object]](fieldInTuple)
            fieldInTuple += 1
            fieldIndex += 1
          }
        }
        tupleIndex += 1
      }
      rowToReturn
    }
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    throw new Exception("UnionOperator.processPartition() should've never been called.")
  }
}

