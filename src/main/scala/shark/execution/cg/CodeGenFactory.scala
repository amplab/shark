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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.cg.node._
import shark.execution.cg.udf._

class CodeGenFactory (val context:CGContext){ 
  private def makeBetweenUDF(node : GenericFunExprNodeCodeGen) = {
    val exprs = node.getDesc().getChildExprs()
    val desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
               TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">=",exprs.get(1), exprs.get(2)),
               TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=",exprs.get(1), exprs.get(3))).asInstanceOf[ExprNodeGenericFuncDesc]
    
    node.getContext().create(desc)
  }
  
  def create(node : GenericFunExprNodeCodeGen) : ExprCodeGen = {
    node.genericUDF match {
      // 1. Logical Operators
      case x : GenericUDFOPAnd                => new GenericUDFOPAndCodeGen(node) // AND (&&)
      case x : GenericUDFOPOr                 => new GenericUDFOPOrCodeGen(node) // OR  (||)
      case x : GenericUDFOPNot                => new GenericUDFOPNotCodeGen(node) // NOT (! )
      case x : GenericUDFOPEqualNS            => new GenericUDFOPEqualNSCodeGen(node) // <=>
      case x : GenericUDFOPEqual              => new GenericUDFOPEqualCodeGen(node) // =
      case x : GenericUDFOPNotEqual           => new GenericUDFOPNotEqualCodeGen(node) // <> (!=)
      case x : GenericUDFOPLessThan           => new GenericUDFOPLessThanCodeGen(node) // <
      case x : GenericUDFOPEqualOrLessThan    => new GenericUDFOPEqualOrLessThanCodeGen(node) // <=
      case x : GenericUDFOPGreaterThan        => new GenericUDFOPGreaterThanCodeGen(node) // >
      case x : GenericUDFOPEqualOrGreaterThan => new GenericUDFOPEqualOrGreaterThanCodeGen(node) // >=
      case x : GenericUDFBetween              => makeBetweenUDF(node) // between
      case x : GenericUDFOPNull               => new GenericUDFOPNullCodeGen(node) // IS NULL
      case x : GenericUDFOPNotNull            => new GenericUDFOPNotNullCodeGen(node) // IS NOT NULL
      case x : GenericUDFPrintf               => new GenericUDFPrintfCodeGen(node) // printf
      case x : GenericUDFInstr                => new GenericUDFInstrCodeGen(node) // instr
      case x : GenericUDFBridge               => create(x, node) // bridge
      case _                                  => null // call to the Generic UDF function
    }
  }

  def create(udf : GenericUDFBridge, node : GenericFunExprNodeCodeGen) : ExprCodeGen = {
    val t = udf.getUdfClass().newInstance()
    t match {
      case x : UDFOPPlus       => new UDFOPPlusBinaryUDFCodeGen(node) // +
      case x : UDFOPMinus      => new UDFOPMinusBinaryUDFCodeGen(node) // -
      case x : UDFOPMultiply   => new UDFOPMultiplyBinaryUDFCodeGen(node) // *
      case x : UDFOPDivide     => new UDFOPDivideBinaryUDFCodeGen(node) // /
      case x : UDFOPLongDivide => new UDFOPLongDivideBinaryUDFCodeGen(node) // / to the long integer
      case x : UDFOPMod        => new UDFOPModBinaryUDFCodeGen(node) // %
      case x : UDFPosMod       => new UDFPosModBinaryUDFCodeGen(node) // pmod ((a % b) + b) % b
      case x : UDFOPBitAnd     => new UDFOPBitAndBinaryUDFCodeGen(node) // &
      case x : UDFOPBitOr      => new UDFOPBitOrBinaryUDFCodeGen(node) // |
      case x : UDFOPBitXor     => new UDFOPBitXorBinaryUDFCodeGen(node) // ^
      case x : UDFOPBitNot     => new UDFOPBitNotBinaryUDFCodeGen(node) // ~
      case x : UDF             => new BridgeUDFCodeGen(udf, node) // UDF
      case _                   => null
    }
  }

  def create(outputOI : ConstantObjectInspector) : ExprNodeCodeGen[ExprNodeDesc] = 
    new ConstantExprNodeCodeGen(context, outputOI)
  
    def create(desc : ExprNodeDesc) : ExprNodeCodeGen[ExprNodeDesc] = {
    var wrapper = new ExprNodeDescEqualityWrapper(desc)
    var codegen : ExprNodeCodeGen[ExprNodeDesc] = null

    var idx = descs.indexOf(wrapper)
    if ((idx < 0) ||
      (codegens.get(idx).isInstanceOf[GenericFunExprNodeCodeGen] &&
        (codegens.get(idx).asInstanceOf[GenericFunExprNodeCodeGen]).isDeterministic() == false)) {
      // doens't exist
      desc match {
        case x : ExprNodeGenericFuncDesc => codegen = new GenericFunExprNodeCodeGen(context, x)
        case x : ExprNodeColumnDesc      => codegen = new ColumnExprNodeCodeGen(context, x)
        case x : ExprNodeConstantDesc    => codegen = new ConstantExprNodeCodeGen(context, x)
        case x : ExprNodeFieldDesc       => codegen = new FieldExprNodeCodeGen(context, x)
        case x : ExprNodeNullDesc        => codegen = new NullExprNodeExprCodeGen(context, x)
      }

      // add to last;
      descs.add(wrapper)
      codegens.add(codegen)
    } else {
      codegen = codegens.get(idx)
    }

    codegen
  }

  private[this] val descs = new ArrayList[ExprNodeDescEqualityWrapper]()
  private[this] val codegens = new ArrayList[ExprNodeCodeGen[ExprNodeDesc]]()
}