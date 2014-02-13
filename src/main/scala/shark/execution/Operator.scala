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

import scala.language.existentials

import java.util.{List => JavaList}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator

import org.apache.spark.rdd.RDD

import shark.LogHelper
import shark.execution.serialization.OperatorSerializationWrapper


abstract class Operator[+T <: HiveDesc] extends LogHelper with Serializable {

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
    objectInspectors = inputObjectInspectors()
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
  def parentOperatorsAsJavaList: JavaList[Operator[_<:HiveDesc]] = _parentOperators

  def addParent(parent: Operator[_<:HiveDesc]) {
    _parentOperators += parent
    parent.childOperators += this
  }

  def addChild(child: Operator[_<:HiveDesc]) {
    child.addParent(this)
  }

  def returnTerminalOperators(): Seq[Operator[_<:HiveDesc]] = {
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

  def desc = _desc

  def setDesc[B >: T](d: B) {_desc = d.asInstanceOf[T]}
  
  @transient private[this] var _desc: T = _
  @transient private val _childOperators = new ArrayBuffer[Operator[_<:HiveDesc]]()
  @transient private val _parentOperators = new ArrayBuffer[Operator[_<:HiveDesc]]()
  @transient var objectInspectors: Seq[ObjectInspector] =_

  protected def executeParents(): Seq[(Int, RDD[_])] = {
    parentOperators.map(p => (p.getTag, p.execute()))
  }
  
  protected def inputObjectInspectors(): Seq[ObjectInspector] = {
    if (null != _parentOperators) {
      _parentOperators.sortBy(_.getTag).map(_.outputObjectInspector)
    } else {
      Seq.empty[ObjectInspector]
    }
  }
  
  // derived classes can set this to different object if needed, default is the first input OI
  def outputObjectInspector(): ObjectInspector = objectInspectors(0)
  
  /**
   * Copy from the org.apache.hadoop.hive.ql.exec.ReduceSinkOperator
   * Initializes array of ExprNodeEvaluator. Adds Union field for distinct
   * column indices for group by.
   * Puts the return values into a StructObjectInspector with output column
   * names.
   *
   * If distinctColIndices is empty, the object inspector is same as
   * {@link Operator#initEvaluatorsAndReturnStruct(ExprNodeEvaluator[], List, ObjectInspector)}
   */
  protected def initEvaluatorsAndReturnStruct(
      evals: Array[ExprNodeEvaluator] , distinctColIndices: JavaList[JavaList[Integer]] ,
      outputColNames: JavaList[String], length: Int, rowInspector: ObjectInspector): 
      StructObjectInspector = {

    val fieldObjectInspectors = initEvaluators(evals, 0, length, rowInspector);
    initEvaluatorsAndReturnStruct(evals, fieldObjectInspectors, distinctColIndices, 
      outputColNames, length, rowInspector)
  }
  
  /**
   * Copy from the org.apache.hadoop.hive.ql.exec.ReduceSinkOperator
   * Initializes array of ExprNodeEvaluator. Adds Union field for distinct
   * column indices for group by.
   * Puts the return values into a StructObjectInspector with output column
   * names.
   *
   * If distinctColIndices is empty, the object inspector is same as
   * {@link Operator#initEvaluatorsAndReturnStruct(ExprNodeEvaluator[], List, ObjectInspector)}
   */
  protected def initEvaluatorsAndReturnStruct(
      evals: Array[ExprNodeEvaluator], fieldObjectInspectors: Array[ObjectInspector], 
      distinctColIndices: JavaList[JavaList[Integer]], outputColNames: JavaList[String], 
      length: Int, rowInspector: ObjectInspector): StructObjectInspector = {

    val inspectorLen = if (evals.length > length) length + 1 else evals.length
    val sois = new ArrayBuffer[ObjectInspector](inspectorLen)

    // keys
    // var fieldObjectInspectors = initEvaluators(evals, 0, length, rowInspector);
    sois ++= fieldObjectInspectors

    if (outputColNames.size > length) {
      // union keys
      val uois = new ArrayBuffer[ObjectInspector]()
      for (/*List<Integer>*/ distinctCols <- distinctColIndices) {
        val names = new ArrayBuffer[String]()
        val eois = new ArrayBuffer[ObjectInspector]()
        var numExprs = 0
        for (i <- distinctCols) {
          names.add(HiveConf.getColumnInternalName(numExprs))
          eois.add(evals(i).initialize(rowInspector))
          numExprs += 1
        }
        uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(names, eois))
      }
      
      sois.add(ObjectInspectorFactory.getStandardUnionObjectInspector(uois))
    }
    
    ObjectInspectorFactory.getStandardStructObjectInspector(outputColNames, sois)
  }
  
  /**
   * Initialize an array of ExprNodeEvaluator and return the result
   * ObjectInspectors.
   */
  protected def initEvaluators(evals: Array[ExprNodeEvaluator],
      rowInspector: ObjectInspector): Array[ObjectInspector] = {
    val result = new Array[ObjectInspector](evals.length)
    for (i <- 0 to evals.length -1) {
      result(i) = evals(i).initialize(rowInspector)
    }
    
    result
  }
  
  /**
   * Initialize an array of ExprNodeEvaluator from start, for specified length
   * and return the result ObjectInspectors.
   */
  protected def initEvaluators(evals: Array[ExprNodeEvaluator],
      start: Int, length: Int,rowInspector: ObjectInspector): Array[ObjectInspector] = {
    val result = new Array[ObjectInspector](length)
    
    for (i <- 0 to length - 1) {
      result(i) = evals(start + i).initialize(rowInspector)
    }
    
    result
  }
  
  /**
   * Initialize an array of ExprNodeEvaluator and put the return values into a
   * StructObjectInspector with integer field names.
   */
  protected def initEvaluatorsAndReturnStruct(
      evals: Array[ExprNodeEvaluator], outputColName: JavaList[String],
      rowInspector: ObjectInspector): StructObjectInspector = {
    val fieldObjectInspectors = initEvaluators(evals, rowInspector)
    return ObjectInspectorFactory.getStandardStructObjectInspector(
      outputColName, fieldObjectInspectors.toList)
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
abstract class NaryOperator[T <: HiveDesc] extends Operator[T] {

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
abstract class UnaryOperator[T <: HiveDesc] extends Operator[T] {

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


abstract class TopOperator[T <: HiveDesc] extends UnaryOperator[T]


object Operator extends LogHelper {

  /** A reference to HiveConf for convenience. */
  @transient var hconf: HiveConf = _

  /**
   * Calls the code to process the partitions. It is placed here because we want
   * to do logging, but calling logging automatically adds a reference to the
   * operator (which is not serializable by Java) in the Spark closure.
   */
  def executeProcessPartition(operator: Operator[_ <: HiveDesc], rdd: RDD[_]): RDD[_] = {
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

