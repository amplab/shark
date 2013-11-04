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

import java.util.{ArrayList, List => JavaList}

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.plan.UnionDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.rdd.{RDD, UnionRDD}

import shark.execution.serialization.OperatorSerializationWrapper


/**
 * A union operator. If the incoming data are of different type, the union
 * operator transforms the incoming data into the same type.
 */
class UnionOperator extends NaryOperator[UnionDesc] {

  @transient var parentFields: Seq[JavaList[_ <: StructField]] = _
  @transient var parentObjInspectors: Seq[StructObjectInspector] = _
  @transient var columnTypeResolvers: Array[ReturnObjectInspectorResolver] = _
  @transient var outputObjInspector: ObjectInspector = _

  @BeanProperty var needsTransform: Array[Boolean] = _
  @BeanProperty var numParents: Int = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    numParents = parentOperators.size

    // whether we need to do transformation for each parent
    var parents = parentOperators.length
    var outputOI = outputObjectInspector()
    needsTransform = Array.tabulate[Boolean](objectInspectors.length) { i =>
      // ObjectInspectors created by the ObjectInspectorFactory, 
      // which take the same ref if equals
      objectInspectors(i) != outputOI
    }
    
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    // Some how in union, it is possible for Hive to add an extra null object
    // inspectors. We need to work around that.
    parentObjInspectors = objectInspectors.filter(_ != null)
        .map(_.asInstanceOf[StructObjectInspector])
    parentFields = parentObjInspectors.map(_.getAllStructFieldRefs())

    // Get columnNames from the first parent
    val numColumns = parentFields.head.size()
    val columnNames = parentFields.head.map(_.getFieldName())

    // Get outputFieldOIs
    columnTypeResolvers = Array.fill(numColumns)(new ReturnObjectInspectorResolver(true))

    for (p <- 0 until numParents) {
      assert(parentFields(p).size() == numColumns)
      for (c <- 0 until numColumns) {
        columnTypeResolvers(c).update(parentFields(p).get(c).getFieldObjectInspector())
      }
    }

    val outputFieldOIs = columnTypeResolvers.map(_.get())
    outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      columnNames, outputFieldOIs.toList)

    // whether we need to do transformation for each parent
    // We reuse needsTransform from Hive because the comparison of object
    // inspectors are hard once we send object inspectors over the wire.
    needsTransform.zipWithIndex.filter(_._1).foreach { case(transform, p) =>
      logDebug("Union Operator needs to transform row from parent[%d] from %s to %s".format(
        p, objectInspectors(p), outputObjInspector))
    }
  }

  override def outputObjectInspector() = outputObjInspector

  /**
   * Override execute. The only thing we need to call is combineMultipleRdds().
   */
  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    val rddsInOrder: Seq[RDD[Any]] = rdds.sortBy(_._1).map(_._2.asInstanceOf[RDD[Any]])

    val rddsTransformed = rddsInOrder.zipWithIndex.map { case(rdd, tag) =>
      if (needsTransform(tag)) {
        transformRdd(rdd, tag)
      } else {
        rdd
      }
    }

    new UnionRDD(rddsTransformed.head.context, rddsTransformed.asInstanceOf[Seq[RDD[Any]]])
  }

  def transformRdd(rdd: RDD[_], tag: Int) = {
    // Since Union does not rely on the general Operator structure, we need
    // to serialize the object inspectors ourselves.
    val op = OperatorSerializationWrapper(this)

    rdd.mapPartitions { part =>
      op.initializeOnSlave()

      val numColumns = op.parentFields.head.size()
      val outputRow = new ArrayList[Object](numColumns)
      for (c <- 0 until numColumns) outputRow.add(null)

      part.map { row =>
        val soi = op.parentObjInspectors(tag)
        val fields = op.parentFields(tag)

        for (c <- 0 until fields.size) {
          outputRow.set(c, op.columnTypeResolvers(c).convertIfNecessary(soi
              .getStructFieldData(row, fields.get(c)), fields.get(c)
              .getFieldObjectInspector()))
        }

        outputRow
      }
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
    throw new Exception("UnionOperator.processPartition() should've never been called.")
  }
}

