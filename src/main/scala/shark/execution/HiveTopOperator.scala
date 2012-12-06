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

import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.LogHelper


/**
 * Operators that are top operators in Hive stages. This includes TableScan and
 * everything that can come after ReduceSink. Note that they might have multiple
 * upstream operators (multiple parents).
 */
trait HiveTopOperator extends LogHelper {
  self: Operator[_ <: HiveOperator] =>

  /**
   * Stores the input object inspectors. This is passed down by either the
   * upstream operators (i.e. ReduceSink) or in the case of TableScan, passed
   * by the init code in SparkTask.
   */
  @transient
  val inputObjectInspectors = new scala.collection.mutable.HashMap[Int, ObjectInspector]

  /**
   * Stores the deser for operators downstream from ReduceSink. This is set by
   * ReduceSink.initializeDownStreamHiveOperators().
   */
  @transient
  val keyValueTableDescs = new scala.collection.mutable.HashMap[Int, (TableDesc, TableDesc)]

  /**
   * Initialize the Hive operator when all input object inspectors are ready.
   */
  def initializeHiveTopOperator() {
    logInfo("Started executing " + self + " initializeHiveTopOperator()")

    // Call initializeDownStreamHiveOperators() of upstream operators that are
    // ReduceSink so we can get the proper input object inspectors and serdes.
    val reduceSinkParents = self.parentOperators.filter(_.isInstanceOf[ReduceSinkOperator])
    reduceSinkParents.foreach { parent =>
      parent.asInstanceOf[ReduceSinkOperator].initializeDownStreamHiveOperator()
      logInfo("parent : " + parent)
    }
    
    // Only do initialize if all our input inspectors are ready. We use >
    // instead of == since TableScan doesn't have parents, but have an object
    // inspector. If == is used, table scan is skipped.
    assert(inputObjectInspectors.size >= reduceSinkParents.size,
      println("# input object inspectors (%d) < # reduce sink parent operators (%d)".format(
          inputObjectInspectors.size, reduceSinkParents.size)))

    val objectInspectorArray = {
      // Special case for single object inspector (non join case) because the
      // joinTag is -1.
      if (inputObjectInspectors.size == 1) {
        Array(inputObjectInspectors.values.head)
      } else {
        val arr = new Array[ObjectInspector](inputObjectInspectors.size)
        inputObjectInspectors foreach { case (tag, inspector) => arr(tag) = inspector }
        arr
      }
    }

    if (objectInspectorArray.size > 0) {    
      // Initialize the hive operators. This init propagates downstream.
      logDebug("Executing " + self.hiveOp + ".initialize()")
      self.hiveOp.initialize(hconf, objectInspectorArray)
    }
    
    logInfo("Finished executing " + self + " initializeHiveTopOperator()")
  }

  def setInputObjectInspector(tag: Int, objectInspector: ObjectInspector) {
    inputObjectInspectors.put(tag, objectInspector)
  }
  
  def setKeyValueTableDescs(tag: Int, descs: (TableDesc, TableDesc)) {
    keyValueTableDescs.put(tag, descs)
  }

}

