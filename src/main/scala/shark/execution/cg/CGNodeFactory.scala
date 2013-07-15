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

package shark.execution.cg

import java.util.ArrayList

import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf._
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.cg.node._
import shark.execution.cg.udf._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer


/**
 * Utilities to create the ExprNode or UDF object under its context. 
 */
class CGNodeFactory(val context: CGContext) {
  def create(node: GenericFunNode): ExprCodeGen = {
    node.genericUDF match {
      case x: GenericUDFOPAnd                => UDFOPAnd(node) // AND (&&)
      case x: GenericUDFOPOr                 => UDFOPOr(node) // OR  (||)
      case x: GenericUDFOPNot                => UDFOPNot(node) // NOT (! )
      case x: GenericUDFOPEqualNS            => UDFOPEqualNS(node) // <=>
      case x: GenericUDFOPEqual              => UDFOPEqual(node) // =
      case x: GenericUDFOPNotEqual           => UDFOPNotEqual(node) // <> (!=)
      case x: GenericUDFOPLessThan           => UDFOPLessThan(node) // <
      case x: GenericUDFOPEqualOrLessThan    => UDFOPEqualOrLessThan(node) // <=
      case x: GenericUDFOPGreaterThan        => UDFOPGreaterThan(node) // >
      case x: GenericUDFOPEqualOrGreaterThan => UDFOPEqualOrGreaterThan(node) // >=
      case x: GenericUDFBetween              => UDFOPBetween(node) // between
      case x: GenericUDFOPNull               => UDFOPNull(node) // IS NULL
      case x: GenericUDFOPNotNull            => UDFOPNotNull(node) // IS NOT NULL
      case x: GenericUDFPrintf               => UDFPrintf(node) // printf
      case x: GenericUDFInstr                => UDFInstr(node) // instr
      case x: GenericUDFCase                 => UDFCase(node) // case
      case x: GenericUDFWhen                 => UDFWhen(node) // when
      case x: GenericUDFIf                   => UDFIf(node) // if
      // TODO as un-recognized genericUDF will lead to code gen fails, and resort to Hive Expr Evals
      // most likely more frequently used GenericUDF would be added in the future.
      case x: GenericUDFBridge               => create(x, node) // bridge
      case _                                 => null // call to the Generic UDF function
    }
  }

  def create(udf: GenericUDFBridge, node: GenericFunNode): ExprCodeGen = {
    udf.getUdfClass().newInstance() match {
      case x: UDFOPPlus       => UDFOPPlusBinaryUDF(node) // +
      case x: UDFOPMinus      => UDFOPMinusBinaryUDF(node) // -
      case x: UDFOPMultiply   => UDFOPMultiplyBinaryUDF(node) // *
      case x: UDFOPDivide     => UDFOPDivideBinaryUDF(node) // /
      case x: UDFOPLongDivide => UDFOPLongDivideBinaryUDF(node) // / to the long integer
      case x: UDFOPMod        => UDFOPModBinaryUDF(node) // %
      case x: UDFPosMod       => UDFPosModBinaryUDF(node) // pmod ((a % b) + b) % b
      case x: UDFOPBitAnd     => UDFOPBitAndBinaryUDF(node) // &
      case x: UDFOPBitOr      => UDFOPBitOrBinaryUDF(node) // |
      case x: UDFOPBitXor     => UDFOPBitXorBinaryUDF(node) // ^
      case x: UDFOPBitNot     => UDFOPBitNotBinaryUDF(node) // ~
      // un-recognized UDF will resort to the BridgeUDFCodeGen, which generate the UDF invoking 
      // code snippet in the generated java class
      case x: UDF             => BridgeUDF(udf, node) // UDF
      case _                  => null
    }
  }

  /**
   * create the ExprNode , via the ExprNodeDesc
   * In order to save time for the common sub expression nodes evaluating, identical ExprNodeDesc
   * will get the same ExprNode instance. but there is an exception, if it's the 
   * ExprNodeGenericFuncDesc and deterministic-less 
   * (e.g. UDF rand() may always returns different value for each invokings)  
   */
  def create(desc: ExprNodeDesc): ExprNode[ExprNodeDesc] = {
    find(desc) match {
      case Some(x) => x
      case None => {
        var node: ExprNode[ExprNodeDesc] = null
        desc match {
          case x: ExprNodeGenericFuncDesc => node = GenericFunNode(context, x)
          case x: ExprNodeColumnDesc      => node = ColumnNode(context, x)
          case x: ExprNodeConstantDesc    => node = ConstantNode(context, x)
          case x: ExprNodeFieldDesc       => node = FieldNode(context, x)
          case x: ExprNodeNullDesc        => node = NullNode(context, x)
        }
        add(desc, node)
        
        node
      }
    }
  }
  
  /**
   * Retrieving the ExprCodeGen instance by identical ExprNodeDesc, except for 
   * 1) Deterministic-less function node
   * 2) NullExprNode 
   */
  private def find(desc:ExprNodeDesc) = {
    tuples.find(_._1 == new ExprNodeDescEqualityWrapper(desc)) match {
      case Some((_, node:ExprNode[_])) => 
        if (node.isInstanceOf[GenericFunNode] && 
            node.asInstanceOf[GenericFunNode].isDeterministic() == false)
          None
        else if (node.isInstanceOf[NullNode])
          None
        else
          Some(node)
      case _ => None
    }
  }
  
  // add desc/node tuples
  private def add(desc: ExprNodeDesc, node: ExprNode[ExprNodeDesc]) = 
    tuples += ((new ExprNodeDescEqualityWrapper(desc), node)) 
  
  // tuples of (desc, node) 
  private val tuples = new ArrayBuffer[(_, _)]()
}
