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

import java.util.{List => JavaList, ArrayList =>JavaArrayList}

import scala.beans.BeanProperty
import scala.reflect.ClassTag

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.exec.{JoinUtil => HiveJoinUtil}
import org.apache.hadoop.hive.ql.plan.{JoinCondDesc, JoinDesc}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}

import shark.SharkConfVars


abstract class CommonJoinOperator[T <: JoinDesc] extends NaryOperator[T] with JoinFilter[T] {

  @BeanProperty var conf: T = _
  // Order in which the results should be output.
  @BeanProperty var order: Array[java.lang.Byte] = _
  // condn determines join property (left, right, outer joins).
  @BeanProperty var joinConditions: Array[JoinCondDesc] = _
  @BeanProperty var numTables: Int = _
  @BeanProperty var nullCheck: Boolean = _

  @transient
  var tagLen: Int = _
  @transient
  var joinVals: Array[JavaList[ExprNodeEvaluator]] = _
  @transient
  var joinFilters: Array[JavaList[ExprNodeEvaluator]] = _
  @transient
  var joinValuesObjectInspectors: Array[JavaList[ObjectInspector]] = _
  @transient
  var joinFilterObjectInspectors: Array[JavaList[ObjectInspector]] = _
  @transient
  var joinValuesStandardObjectInspectors: Array[JavaList[ObjectInspector]] = _

  @transient var noOuterJoin: Boolean = _
  @transient var filterMap: Array[Array[Int]] = _
  
  @transient var rowBuffer: Array[AnyRef] = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    conf = desc
    
    order = conf.getTagOrder()
    joinConditions = conf.getConds()
    numTables = parentOperators.size
    nullCheck = SharkConfVars.getBoolVar(hconf, SharkConfVars.JOIN_CHECK_NULL)

    assert(joinConditions.size + 1 == numTables)
  }

  override def initializeOnSlave() {

    noOuterJoin = conf.isNoOuterJoin
    filterMap = conf.getFilterMap

    tagLen = conf.getTagLength()

    joinVals = new Array[JavaList[ExprNodeEvaluator]](tagLen)
    HiveJoinUtil.populateJoinKeyValue(
      joinVals, conf.getExprs(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinFilters = new Array[JavaList[ExprNodeEvaluator]](tagLen)
    HiveJoinUtil.populateJoinKeyValue(
      joinFilters, conf.getFilters(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinValuesObjectInspectors = HiveJoinUtil.getObjectInspectorsFromEvaluators(
      joinVals, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE, tagLen)
    joinFilterObjectInspectors = HiveJoinUtil.getObjectInspectorsFromEvaluators(
      joinFilters, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE, tagLen)
    joinValuesStandardObjectInspectors = HiveJoinUtil.getStandardObjectInspectors(
      joinValuesObjectInspectors, CommonJoinOperator.NOTSKIPBIGTABLE, tagLen)
      
    rowBuffer = new Array[AnyRef](resultRowSize)
  }
  
  // copied from the org.apache.hadoop.hive.ql.exec.CommonJoinOperator
  override def outputObjectInspector() = {
    val structFieldObjectInspectors = new JavaArrayList[ObjectInspector]()
    for (alias <- order) {
      val oiList = joinValuesStandardObjectInspectors(alias.intValue)
      structFieldObjectInspectors.addAll(oiList)
    }

    ObjectInspectorFactory.getStandardStructObjectInspector(
      conf.getOutputColumnNames(),
      structFieldObjectInspectors)
  }
  
  @inline def filterEval(data: AnyRef): Boolean = {
    if (noOuterJoin) false else CommonJoinOperator.filterEval(data)
  }
}


class CartesianProduct[T >: Null : ClassTag](val numTables: Int) {

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
            product2FullOuterJoin(partial, bufs(joinCondition.getRight), i)
          }
        case CommonJoinOperator.LEFT_OUTER_JOIN =>
          if (bufs(joinCondition.getLeft()).size == 0) {
            createBase(EMPTY_LIST, i)
          } else if (bufs(joinCondition.getRight).size == 0) {
            product2(partial, SINGLE_NULL_LIST, i)
          } else {
            product2LeftOuterJoin(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.RIGHT_OUTER_JOIN =>
          if (bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else if (bufs(joinCondition.getLeft).size == 0 || !partial.hasNext) {
            product2(createBase(SINGLE_NULL_LIST, i - 1), bufs(joinCondition.getRight), i)
          } else {
            product2RightOuterJoin(partial, bufs(joinCondition.getRight), i)
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
  
  @inline
  private def filter[B](iter: Iterator[B], eval: (B) => Boolean = CommonJoinOperator.filterEval _)
  : Iterator[B] = {
    var occurs = 1
    iter.filter { e =>
      // Per outer join semantic, on more than 1 null table value allowed, we need to filter out
      // the entries from the iterator if it's failed in join filter testing (just keep 1)
      val discard = eval(e)
      if (discard) {
        occurs = occurs - 1
        // if first appearance
        occurs >= 0
      } else {
        true
      }
    }
  }
  
  def product2(left: Iterator[Array[T]], right: Seq[T], pos: Int): Iterator[Array[T]] = {
    for (l <- left; r <- right.iterator) yield {
      outputBuffer(pos) = r
      outputBuffer
    }
  }
  
  def product2FullOuterJoin(left: Iterator[Array[T]], right: Seq[T], pos: Int): Iterator[Array[T]] =
  {
    left.flatMap { e =>
      if (CommonJoinOperator.filterEval(e(pos - 1))) {
        outputBuffer(pos) = null
        Iterator(outputBuffer)
      } else {
        right.filter(!CommonJoinOperator.filterEval(_)).iterator.map(entry => {
          outputBuffer(pos) = entry
          outputBuffer
        })
      } 
    } ++ right.filter(CommonJoinOperator.filterEval(_)).iterator.flatMap { entry =>
      outputBuffer(pos) = entry
      outputBuffer(pos - 1) = null

      Iterator(outputBuffer)
	  }
  }
  
  def product2LeftOuterJoin(left: Iterator[Array[T]], right: Seq[T], pos: Int)
  : Iterator[Array[T]] = {
    for (lt <- left;
      rt <- filter((if(CommonJoinOperator.filterEval(lt(pos - 1)))
        SINGLE_NULL_LIST else right).iterator)) yield {
      outputBuffer(pos) = rt
      outputBuffer
    }
  }
  
  def product2RightOuterJoin(left: Iterator[Array[T]], right: Seq[T], pos: Int)
  : Iterator[Array[T]] = {

    right.filter(CommonJoinOperator.filterEval(_)).iterator.map { entry =>
      outputBuffer(pos - 1) = null
      outputBuffer(pos) = entry
      outputBuffer
    } ++ filter(product2(left, right.filter(!CommonJoinOperator.filterEval(_)), pos),
      (e: Array[T]) => CommonJoinOperator.filterEval(e(pos - 1)))
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

  // get the evaluated value(boolean) from the table data (the last element in the array)
  // true means failed in the join filter testing, we may need to skip it
  @inline final def filterEval[B](data: B): Boolean = {
    if (data == null) {
      true
    } else {
      val fields = data.asInstanceOf[Array[AnyRef]]
      fields(fields.length - 1).asInstanceOf[org.apache.hadoop.io.BooleanWritable].get
    }
  }
}

