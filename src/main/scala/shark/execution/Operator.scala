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

import java.util.{List => JavaList}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import org.apache.spark.rdd.RDD

import shark.LogHelper
import shark.execution.serialization.OperatorSerializationWrapper


abstract class Operator[T <: HiveOperator] extends LogHelper with Serializable {

  /**
   * Initialize the operator on master node. This can have dependency on other
   * nodes. When an operator's initializeOnMaster() is invoked, all its parents'
   * initializeOnMaster() have been invoked.
   */
  def initializeOnMaster() {}

  /**
   * Initialize the operator on slave nodes. This method should have no
   * dependency on parents or children. Everything that is not used in this
   * method should be marked @transient.
   */
  def initializeOnSlave() {}

  def processPartition(split: Int, iter: Iterator[_]): Iterator[_]

  /**
   * Execute the operator. This should recursively execute parent operators.
   */
  def execute(): RDD[_]

  /**
   * Recursively calls initializeOnMaster() for the entire query plan. Parent
   * operators are called before children.
   */
  def initializeMasterOnAll() {
    _parentOperators.foreach(_.initializeMasterOnAll())
    objectInspectors ++= hiveOp.getInputObjInspectors()
    initializeOnMaster()
  }

  /**
   * Return the join tag. This is usually just 0. ReduceSink might set it to
   * something else.
   */
  def getTag: Int = 0

  def hconf = Operator.hconf

  def childOperators = _childOperators
  def parentOperators = _parentOperators

  /**
   * Return the parent operators as a Java List. This is for interoperability
   * with Java. We use this in explain's Java code.
   */
  def parentOperatorsAsJavaList: JavaList[Operator[_]] = _parentOperators

  def addParent(parent: Operator[_]) {
    _parentOperators += parent
    parent.childOperators += this
  }

  def addChild(child: Operator[_]) {
    child.addParent(this)
  }

  def returnTerminalOperators(): Seq[Operator[_]] = {
    if (_childOperators == null || _childOperators.size == 0) {
      Seq(this)
    } else {
      _childOperators.flatMap(_.returnTerminalOperators())
    }
  }

  def returnTopOperators(): Seq[Operator[_]] = {
    if (_parentOperators == null || _parentOperators.size == 0) {
      Seq(this)
    } else {
      _parentOperators.flatMap(_.returnTopOperators())
    }
  }

  @transient var hiveOp: T = _
  @transient private val _childOperators = new ArrayBuffer[Operator[_]]()
  @transient private val _parentOperators = new ArrayBuffer[Operator[_]]()
  @transient var objectInspectors = new ArrayBuffer[ObjectInspector]

  protected def executeParents(): Seq[(Int, RDD[_])] = {
    parentOperators.map(p => (p.getTag, p.execute()))
  }
}


/**
 * A base operator class that has many parents and one child. This can be used
 * to implement join, union, etc. Operators implementations should override the
 * following methods:
 *
 * combineMultipleRdds: Combines multiple RDDs into a single RDD. E.g. in the
 * case of join, this function does the join operation.
 *
 * processPartition: Called on each slave on the output of combineMultipleRdds.
 * This can be used to transform rows into their desired format.
 *
 * postprocessRdd: Called on the master to transform the output of
 * processPartition before sending it downstream.
 *
 */
abstract class NaryOperator[T <: HiveOperator] extends Operator[T] {

  /** Process a partition. Called on slaves. */
  def processPartition(split: Int, iter: Iterator[_]): Iterator[_]

  /** Called on master. */
  def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_]

  /** Called on master. */
  def postprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    val singleRdd = combineMultipleRdds(inputRdds)
    val rddProcessed = Operator.executeProcessPartition(this, singleRdd)
    postprocessRdd(rddProcessed)
  }

}


/**
 * A base operator class that has at most one parent.
 * Operators implementations should override the following methods:
 *
 * preprocessRdd: Called on the master. Can be used to transform the RDD before
 * passing it to processPartition. For example, the operator can use this
 * function to sort the input.
 *
 * processPartition: Called on each slave on the output of preprocessRdd.
 * This can be used to transform rows into their desired format.
 *
 * postprocessRdd: Called on the master to transform the output of
 * processPartition before sending it downstream.
 *
 */
abstract class UnaryOperator[T <: HiveOperator] extends Operator[T] {

  /** Process a partition. Called on slaves. */
  def processPartition(split: Int, iter: Iterator[_]): Iterator[_]

  /** Called on master. */
  def preprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  /** Called on master. */
  def postprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  def objectInspector = objectInspectors.head

  def parentOperator = parentOperators.head

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val rddPreprocessed = preprocessRdd(inputRdd)
    val rddProcessed = Operator.executeProcessPartition(this, rddPreprocessed)
    postprocessRdd(rddProcessed)
  }
}


abstract class TopOperator[T <: HiveOperator] extends UnaryOperator[T]


object Operator extends LogHelper {

  /** A reference to HiveConf for convenience. */
  @transient var hconf: HiveConf = _

  /**
   * Calls the code to process the partitions. It is placed here because we want
   * to do logging, but calling logging automatically adds a reference to the
   * operator (which is not serializable by Java) in the Spark closure.
   */
  def executeProcessPartition(operator: Operator[_ <: HiveOperator], rdd: RDD[_]): RDD[_] = {
    val op = OperatorSerializationWrapper(operator)
    rdd.mapPartitionsWithIndex { case(split, partition) =>
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave()
      val newPart = op.processPartition(split, partition)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      newPart
    }
  }

}

