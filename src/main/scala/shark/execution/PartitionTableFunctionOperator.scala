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

package org.apache.hadoop.hive.ql.exec

import java.util.ArrayList

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{PTFOperator => HivePTFOperator}
import org.apache.hadoop.hive.ql.plan.{PTFDeserializer, PTFDesc}
import org.apache.hadoop.hive.ql.plan.PTFDesc._
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, ObjectInspector,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.SerDe

import shark.execution.UnaryOperator

class PartitionTableFunctionOperator extends UnaryOperator[PTFDesc] {

  @BeanProperty var conf: PTFDesc = _
  @BeanProperty var inputPart: PTFPartition = _
  @BeanProperty var outputObjInspector: ObjectInspector = _
  @BeanProperty var localHconf: HiveConf = _
  @transient var currentKeys: KeyWrapper = _
  @transient var newKeys: KeyWrapper = _
  @transient var keyWrapperFactory: KeyWrapperFactory = _

  override def initializeOnMaster() {
    conf = desc
    localHconf = super.hconf
  }

  /**
   * 1. Find out if the operator is invoked at Map-Side or Reduce-side
   * 2. Get the deserialized PartitionedTableFunctionDef
   * 3. Reconstruct the transient variables in PartitionedTableFunctionDef
   * 4. Create input partition to store rows coming from previous operator
   */
  override def initializeOnSlave() {
    initializePTFs(conf)
    inputPart = createFirstPartitionForChain(objectInspector, conf.isMapSide)
    setupKeysWrapper(objectInspectors.head)
  }

  /**
   * Initializes function metadata, starting with the input PTFDesc's PartitionedTableFunctionDef.
   */
  protected def initializePTFs(conf: PTFDesc) {
    val dS = new PTFDeserializer(conf, objectInspectors.head.asInstanceOf[StructObjectInspector],
      localHconf)
    //Walk through the PTFInputDef chain to initialize ShapeDetails, OI etc.
    dS.initializePTFChain(conf.getFuncDef)
  }

  override def outputObjectInspector() = {
    initializePTFs(conf)
    if (conf.isMapSide) {
      val tDef = conf.getStartOfChain
      tDef.getRawInputShape.getOI
    } else {
      conf.getFuncDef.getOutputShape.getOI
    }
  }


  override def processPartition(split: Int, iter: Iterator[_]) = {
    new LazyPTFIterator(iter)
  }

  def processInputPartition(): PTFPartition = {
    val outPart = executeChain(inputPart)
    if (conf.forWindowing) {
      val partition = executeWindowExprs(outPart)
      partition
    } else {
      outPart
    }
  }

  /**
   * For all the table functions to be applied to the input hive table or query,
   * push them on a stack. For each table function popped out of the stack,
   * execute the function on the input partition and return an output partition.
   * @param part
   * @return
   */
  def executeChain(part: PTFPartition): PTFPartition = {
    val fnDefs = new mutable.Stack[PartitionedTableFunctionDef]
    var iDef: PTFInputDef = conf.getFuncDef
    while (iDef.isInstanceOf[PartitionedTableFunctionDef]) {
      val ptfDef = iDef.asInstanceOf[PartitionedTableFunctionDef]
      fnDefs.push(ptfDef)
      iDef = ptfDef.getInput
    }

    var currFnDef: PartitionedTableFunctionDef = null
    var ptfPartition: PTFPartition = part
    while (!fnDefs.isEmpty) {
      currFnDef = fnDefs.pop()
      ptfPartition = currFnDef.getTFunction().execute(ptfPartition)
    }
    ptfPartition
  }

  def executeWindowExprs(oPart: PTFPartition): PTFPartition = {
    val wTFnDef: WindowTableFunctionDef = conf.getFuncDef.asInstanceOf[WindowTableFunctionDef]
    val inputOI = wTFnDef.getOutputFromWdwFnProcessing().getOI
    val outputOI = wTFnDef.getOutputShape().getOI
    val numCols = outputOI.getAllStructFieldRefs().size
    val wdwExprs = wTFnDef.getWindowExpressions
    val numWdwExprs = if (wdwExprs == null) 0 else wdwExprs.size
    val output = new Array[Object](numCols)

    val forwardRowsUntouched = (wdwExprs == null || wdwExprs.size() == 0)
    if (forwardRowsUntouched) {
      return oPart
    }

    val pItr = oPart.iterator
    val newPartition = createFirstPartitionForChain(objectInspector, conf.isMapSide)
    PTFOperator.connectLeadLagFunctionsToPartition(conf, pItr)
    pItr.foreach{ oRow =>
      var colCnt = 0

      if (wdwExprs != null) {
        wdwExprs.foreach { e =>
          output(colCnt) = e.getExprEvaluator.evaluate(oRow)
          colCnt = colCnt + 1
        }
      }

      for (colCnt <- 0 to numCols) {
        val field = inputOI.getAllStructFieldRefs().get(colCnt - numWdwExprs)
        output(colCnt) = ObjectInspectorUtils.copyToStandardObject(
          inputOI.getStructFieldData(oRow, field), field.getFieldObjectInspector())
      }
      newPartition.append(output)
    }
    newPartition
  }

  def processMapFunction(): PTFPartition = {
    conf.getStartOfChain.getTFunction.transformRawInput(inputPart)
  }

  def setupKeysWrapper(inputOI: ObjectInspector) {
    val pDef: PartitionDef = conf.getStartOfChain().getPartition
    val exprs: ArrayList[PTFExpressionDef] = pDef.getExpressions
    val numExprs = exprs.size
    val keyFields = new Array[ExprNodeEvaluator](numExprs)
    val keyOIs = new Array[ObjectInspector](numExprs)
    val currentKeyOIs = new Array[ObjectInspector](numExprs)

    for (i <- 0 until numExprs) {
      val exprDef: PTFExpressionDef = exprs.get(i)
      /*
			 * Why cannot we just use the ExprNodeEvaluator on the column?
			 * - because on the reduce-side it is initialized based on the rowOI of the HiveTable
			 *   and not the OI of the ExtractOp ( the parent of this Operator on the reduce-side)
			 */
      keyFields(i) = ExprNodeEvaluatorFactory.get(exprDef.getExprNode)
      keyOIs(i) = keyFields(i).initialize(inputOI)
      currentKeyOIs(i) =
        ObjectInspectorUtils.getStandardObjectInspector(keyOIs(i),
          ObjectInspectorCopyOption.WRITABLE)
    }

    keyWrapperFactory = new KeyWrapperFactory(keyFields, keyOIs, currentKeyOIs)
    newKeys = keyWrapperFactory.getKeyWrapper
  }

  def createFirstPartitionForChain(oi: ObjectInspector, isMapSide: Boolean): PTFPartition = {
    val tabDef: PartitionedTableFunctionDef = conf.getStartOfChain
    val tEval: TableFunctionEvaluator = tabDef.getTFunction
    val partClassName: String = tEval.getPartitionClass
    val partMemSize = tEval.getPartitionMemSize

    val serde: SerDe = if (isMapSide) {
      tabDef.getInput().getOutputShape().getSerde()
    } else {
      tabDef.getRawInputShape().getSerde
    }
    val part: PTFPartition = new PTFPartition(partClassName, partMemSize, serde,
      oi.asInstanceOf[StructObjectInspector])
    part
  }

  /**
   * as the class name shows, it's very lazy, compute for new PTFPartition
   * after the previous one has been consumed.
   * @param iter
   */
  class LazyPTFIterator(iter: Iterator[_]) extends Iterator[Any] {
    var curIter: Iterator[Any] = _
    var curRow: Any = _
    var complete: Boolean = false

    def hasNext: Boolean = {
      if (curIter == null || !curIter.hasNext) {
        val part = getNextPTFPartition
        if (part == null) {
          return false
        } else {
          curIter = part.iterator
        }
      }

      curIter.hasNext
    }

    def next(): Any = {
      if (hasNext) curIter.next() else null
    }

    /**
     * rows from input iterator would be splited into different PTFParitions by partition keys,
     * you can get each PTFPartition one by one. This method invocation would reset previous
     * PTFPartition, so make sure you do this after you have consumed the previous PTFPartition.
     * @return next PTFPartition
     */
    def getNextPTFPartition(): PTFPartition = {
      if (!iter.hasNext) {
        if(complete) {
           return null
        }
      }

      if (curRow != null) {
        /*
         * if curRow is not null, means this method has been invoked before. So reset inputPart
         * and append curRow which is saved before.
         */
        inputPart.reset()
        inputPart.append(curRow)
      }

      while (iter.hasNext) {
        var partition: PTFPartition = null
        val row = iter.next().asInstanceOf[Any]
        if (!conf.isMapSide) {
          newKeys.getNewKey(row, inputPart.getOI)
          val keysAreEqual = if (currentKeys != null && newKeys != null) {
            newKeys.equals(currentKeys)
          } else {
            false
          }

          if (currentKeys != null && !keysAreEqual) {
            // now we know all row of current PTFPartition has been appended to inputPart.
            // and we save current row for next PTFPartition.
            curRow = row
            partition = processInputPartition()
          }

          if (currentKeys == null || !keysAreEqual) {
            if (currentKeys == null) {
              currentKeys = newKeys.copyKey()
            }
            else {
              currentKeys.copyKey(newKeys)
            }
          }

          if (partition != null) {
            return partition
          }
        }

        // add row to current Partition.
        inputPart.append(row)
      }

      complete = true
      if (conf.isMapSide) processMapFunction() else processInputPartition()
    }
  }
}
