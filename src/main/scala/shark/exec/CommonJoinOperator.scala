package shark.exec

import java.util.{HashMap => JavaHashMap, List => JavaList}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, JoinUtil}
import org.apache.hadoop.hive.ql.exec.{CommonJoinOperator => HiveCommonJoinOperator}
import org.apache.hadoop.hive.ql.plan.{ExprNodeDesc, JoinCondDesc, JoinDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import spark.{CoGroupedRDD, UnionRDD, RDD}
import spark.SparkContext.rddToPairRDDFunctions


abstract class CommonJoinOperator[JOINDESCTYPE <: JoinDesc, T <: HiveCommonJoinOperator[JOINDESCTYPE]]
  extends NaryOperator[T] {

  @BeanProperty var conf: JOINDESCTYPE = _
  // Order in which the results should be output.
  @BeanProperty var order: Array[java.lang.Byte] = _
  // condn determines join property (left, right, outer joins).
  @BeanProperty var joinConditions: Array[JoinCondDesc] = _
  @BeanProperty var numTables: Int = _

  @transient
  var joinVals: JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]] = _
  @transient
  var joinFilters: JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]] = _
  @transient
  var joinValuesObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _
  @transient
  var joinFilterObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _
  @transient
  var joinValuesStandardObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _

  @transient var noOuterJoin: Boolean = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()

    order = conf.getTagOrder()
    joinConditions = conf.getConds()
    numTables = parentOperators.size

    assert(joinConditions.size + 1 == numTables)
  }
  
  override def initializeOnSlave() {

    noOuterJoin = conf.isNoOuterJoin

    joinVals = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    JoinUtil.populateJoinKeyValue(joinVals, conf.getExprs(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinFilters = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    JoinUtil.populateJoinKeyValue(
      joinFilters, conf.getFilters(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(
      joinVals, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(
      joinFilters, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    joinValuesStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(
      joinValuesObjectInspectors, CommonJoinOperator.NOTSKIPBIGTABLE)
  }
}


class CartesianProductIterator(val bufs: IndexedSeq[Seq[Any]]) {
  
}


object CommonJoinOperator {

  val NOTSKIPBIGTABLE = -1

  // Different join types.
  val INNER_JOIN = JoinDesc.INNER_JOIN
  val LEFT_OUTER_JOIN = JoinDesc.LEFT_OUTER_JOIN
  val RIGHT_OUTER_JOIN = JoinDesc.RIGHT_OUTER_JOIN
  val FULL_OUTER_JOIN = JoinDesc.FULL_OUTER_JOIN
  val UNIQUE_JOIN = JoinDesc.UNIQUE_JOIN // We don't support UNIQUE JOIN.
  val LEFT_SEMI_JOIN = JoinDesc.LEFT_SEMI_JOIN

  def join[K: ClassManifest](
    rdds: Seq[RDD[(K, Any)]],
    joinConditions: Array[JoinCondDesc],
    numSplits: Int)
  : RDD[Seq[Any]] = {

    val numTables = rdds.size
    assert(joinConditions.size == numTables - 1)

    val labeledRdds = rdds.zipWithIndex.map { case(rdd, index) =>
      rdd.map { case(k, v) => (k, (index.toByte, v)) }
    }
    val union = new UnionRDD(rdds.head.context, labeledRdds)

    union.groupByKey(numSplits).mapPartitions { part =>
      val bufs = new Array[ArrayBuffer[Any]](numTables)
      for (i <- 0 until numTables) bufs(i) = new ArrayBuffer[Any]

      part.flatMap { case (k, seq) =>
        bufs.foreach(_.clear())
        seq.foreach { case(label, value) =>
          bufs(label) += value
        }
        cartesianProduct(bufs.asInstanceOf[Array[Seq[Any]]], joinConditions)
      }
    }
  }

  def cartesianProduct(bufs: Array[Seq[Any]], joinConditions: Array[JoinCondDesc])
  : Iterator[Seq[Any]] = {

    val tupleSize = bufs.size

    // This can be done with a foldLeft, but it will be too confusing if we
    // need to zip the bufs with a list of join descriptors...
    var partialProduct: Iterator[Seq[Any]] = bufs(joinConditions.head.getLeft()).map(Seq(_)).iterator
    var i = 0
    joinConditions.foreach { joinCondition =>
      i += 1
      val joinType = joinCondition.getType()

      if (joinType == INNER_JOIN) {
        if (bufs(joinCondition.getLeft()).size == 0 || bufs(joinCondition.getRight()).size == 0) {
          partialProduct = Iterator.empty
        } else {
          partialProduct = cartesianProduct2(partialProduct, bufs(joinCondition.getRight()))
        }
      } else if (joinType == FULL_OUTER_JOIN) {

        if (bufs(joinCondition.getLeft()).size == 0 || !partialProduct.hasNext) {
          // If both right/left are empty, then the right side returns an empty
          // iterator and cartesianProduct2 also returns an empty iterator.
          partialProduct = cartesianProduct2(
            Iterator.single(Array.fill[Null](i)(null).toSeq), bufs(joinCondition.getRight()))
        } else if (bufs(joinCondition.getRight()).size == 0) {
          partialProduct = cartesianProduct2(partialProduct, Seq(null))
        } else {
          partialProduct = cartesianProduct2(partialProduct, bufs(joinCondition.getRight()))
        }
      } else if (joinType == LEFT_OUTER_JOIN) {

        if (bufs(joinCondition.getLeft()).size == 0) {
          partialProduct = Iterator.empty
        } else if (bufs(joinCondition.getRight()).size == 0) {
          partialProduct = cartesianProduct2(partialProduct, Seq(null))
        } else {
          partialProduct = cartesianProduct2(
            partialProduct, bufs(joinCondition.getRight()))
        }
      } else if (joinType == RIGHT_OUTER_JOIN) {

        if (bufs(joinCondition.getRight()).size == 0) {
          partialProduct = Iterator.empty
        } else if (bufs(joinCondition.getLeft()).size == 0 || !partialProduct.hasNext) {
          partialProduct = cartesianProduct2(
            Iterator.single(Array.fill[Null](i)(null).toSeq), bufs(joinCondition.getRight()))
        } else {
          partialProduct = cartesianProduct2(partialProduct, bufs(joinCondition.getRight()))
        }
      } else if (joinType == LEFT_SEMI_JOIN) {
        // For semi join, we only need one element from the table on the right
        // to verify an row exists.
        if (bufs(joinCondition.getLeft()).size == 0 || bufs(joinCondition.getRight()).size == 0) {
          partialProduct = Iterator.empty
        } else {
          partialProduct = cartesianProduct2(partialProduct, Seq(null))
        }
      }
    }
    partialProduct
  }

  /**
   * Cartesian product of two.
   */
  def cartesianProduct2(left: Iterator[Seq[Any]], right: Seq[Any]): Iterator[Seq[Any]] = {
    for (l <- left; r <- right.iterator) yield(l :+ r)
  }

  /**
   * Handles join filters in Hive. It is kind of buggy and not used at the moment.
   */
  def isFiltered(row: Any, filters: JavaList[ExprNodeEvaluator], ois: JavaList[ObjectInspector])
  : Boolean = {
    var ret: java.lang.Boolean = false
    var j = 0
    while (j < filters.size) {
      val condition: java.lang.Object = filters.get(j).evaluate(row)
      ret = ois.get(j).asInstanceOf[PrimitiveObjectInspector].getPrimitiveJavaObject(
        condition).asInstanceOf[java.lang.Boolean]
      if (ret == null || !ret) {
        return true;
      }
      j += 1
    }
    false
  }

  /**
   * Determines the order in which the tables should be joined (i.e. the order
   * in which we produce the Cartesian products).
   */
  def computeTupleOrder(joinConditions: Array[JoinCondDesc]): Array[Int] = {
    val tupleOrder = new Array[Int](joinConditions.size + 1)
    var pos = 0

    def addIfNew(table: Int) {
      if (!tupleOrder.contains(table)) {
        tupleOrder(pos) = table
        pos += 1
      }
    }

    joinConditions.foreach { joinCond =>
      addIfNew(joinCond.getLeft())
      addIfNew(joinCond.getRight())
    }
    tupleOrder
  }
}

