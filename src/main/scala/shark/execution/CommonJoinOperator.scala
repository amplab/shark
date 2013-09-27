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

import java.util.{HashMap => JavaHashMap, List => JavaList}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.exec.{CommonJoinOperator => HiveCommonJoinOperator}
import org.apache.hadoop.hive.ql.exec.{JoinUtil => HiveJoinUtil}
import org.apache.hadoop.hive.ql.plan.{ExprNodeDesc, JoinCondDesc, JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import shark.SharkConfVars


abstract class CommonJoinOperator[JOINDESCTYPE <: JoinDesc, T <: HiveCommonJoinOperator[JOINDESCTYPE]]
  extends NaryOperator[T] {

  @BeanProperty var conf: JOINDESCTYPE = _
  // Order in which the results should be output.
  @BeanProperty var order: Array[java.lang.Byte] = _
  // condn determines join property (left, right, outer joins).
  @BeanProperty var joinConditions: Array[JoinCondDesc] = _
  @BeanProperty var numTables: Int = _
  @BeanProperty var nullCheck: Boolean = _

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
    nullCheck = SharkConfVars.getBoolVar(hconf, SharkConfVars.JOIN_CHECK_NULL)

    assert(joinConditions.size + 1 == numTables)
  }

  override def initializeOnSlave() {

    noOuterJoin = conf.isNoOuterJoin

    joinVals = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    HiveJoinUtil.populateJoinKeyValue(
      joinVals, conf.getExprs(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinFilters = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    HiveJoinUtil.populateJoinKeyValue(
      joinFilters, conf.getFilters(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinValuesObjectInspectors = HiveJoinUtil.getObjectInspectorsFromEvaluators(
      joinVals, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    joinFilterObjectInspectors = HiveJoinUtil.getObjectInspectorsFromEvaluators(
      joinFilters, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    joinValuesStandardObjectInspectors = HiveJoinUtil.getStandardObjectInspectors(
      joinValuesObjectInspectors, CommonJoinOperator.NOTSKIPBIGTABLE)
  }
}


class CartesianProduct[T >: Null : ClassManifest](val numTables: Int) {

  val SINGLE_NULL_LIST = Seq[T](null)
  val EMPTY_LIST = Seq[T]()

  // The output buffer array. The product function returns an iterator that will
  // always return this outputBuffer. Downstream operations need to make sure
  // they are just streaming through the output.
  val outputBuffer = new Array[T](numTables)

  def product(bufs: Array[Seq[T]], joinConditions: Array[JoinCondDesc]): Iterator[Array[T]] = {

    // This can be done with a foldLeft, but it will be too confusing if we
    // need to zip the bufs with a list of join descriptors...
    var partial: Iterator[Array[T]] = createBase(bufs(joinConditions.head.getLeft), 0)
    var i = 0
    while (i < joinConditions.length) {
      val joinCondition = joinConditions(i)
      i += 1

      partial = joinCondition.getType() match {
        case CommonJoinOperator.INNER_JOIN =>
          if (bufs(joinCondition.getLeft).size == 0 || bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.FULL_OUTER_JOIN =>
          if (bufs(joinCondition.getLeft()).size == 0 || !partial.hasNext) {
            // If both right/left are empty, then the right side returns an empty
            // iterator and product2 also returns an empty iterator.
            product2(createBase(SINGLE_NULL_LIST, i - 1), bufs(joinCondition.getRight), i)
          } else if (bufs(joinCondition.getRight).size == 0) {
            product2(partial, SINGLE_NULL_LIST, i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.LEFT_OUTER_JOIN =>
          if (bufs(joinCondition.getLeft()).size == 0) {
            createBase(EMPTY_LIST, i)
          } else if (bufs(joinCondition.getRight).size == 0) {
            product2(partial, SINGLE_NULL_LIST, i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.RIGHT_OUTER_JOIN =>
          if (bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else if (bufs(joinCondition.getLeft).size == 0 || !partial.hasNext) {
            product2(createBase(SINGLE_NULL_LIST, i - 1), bufs(joinCondition.getRight), i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.LEFT_SEMI_JOIN =>
          // For semi join, we only need one element from the table on the right
          // to verify an row exists.
          if (bufs(joinCondition.getLeft).size == 0 || bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else {
            product2(partial, SINGLE_NULL_LIST, i)
          }
      }
    }
    partial
  }

  def product2(left: Iterator[Array[T]], right: Seq[T], pos: Int): Iterator[Array[T]] = {
    for (l <- left; r <- right.iterator) yield {
      outputBuffer(pos) = r
      outputBuffer
    }
  }

  def createBase(left: Seq[T], pos: Int): Iterator[Array[T]] = {
    var i = 0
    while (i <= pos) {
      outputBuffer(i) = null
      i += 1
    }
    left.iterator.map { l =>
      outputBuffer(pos) = l
      outputBuffer
    }
  }
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

