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

package shark.execution.cg.udf

import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import shark.execution.cg.node.GenericFunNode
import shark.execution.cg.ValueType
import shark.execution.cg.node.ExprNode
import shark.execution.cg.EvaluationType
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.node.ConverterNode
import shark.execution.cg.node.ConverterType
import shark.execution.cg.ExprCodeGen

/*
 * 1) This file is used to re-implement the generic UDF, any un-recognized UDF will fail the CG and
 * resort to the Hive Expr Eval;
 * 2) The following code could be examples for the new GenericUDF re-implementation
 */

/**
 * printf
 */
class UDFPrintf(node: GenericFunNode) extends UDFCodeGen(node) {

  private var arguments: Array[_<:ExprNode[ExprNodeDesc]] = _
  override def evaluationType() = EvaluationType.SET

  override protected def cgUDFCall() = {
    ()=> {
     var formatterVariable =
      getContext().createValueVariableName(ValueType.TYPE_VALUE, classOf[java.util.Formatter], false)
      
      ("%s = new java.util.Formatter(java.util.Locale.US);\n" +
     "%s.format(%s);\n" +
     "%s.set(%s.toString());\n").format(
       formatterVariable,
       formatterVariable,
       arguments.map(_.valueExpr()).reduce(_ + "," + _),
       resultVariableName(), formatterVariable)
    }
  }

  override protected def requireNullValueCheck(parameterIndex: Int) =
    if (0 == parameterIndex)
      true
    else
      false

  override def prepare(rowInspector: ObjectInspector, children: Array[_ <: ExprNode[ExprNodeDesc]]) = {
    // TODO probably should CHECKED_CAST, which will check the Writable object validity first,
    // but in order to keep aligning with HIVE GenericUDFPrintf, let the NullPointerException
    // throws if null writable object get.
    arguments = children.map(x=>
      ConverterNode(x, x.getOutputInspector(), ConverterType.DIRECT_CAST))

    arguments
  }
}

object UDFPrintf {
  def apply(node: GenericFunNode) = new UDFPrintf(node)
}

/**
 * instr
 */
class UDFInstr(node: GenericFunNode) 
  extends UDFCodeGen(node) {
  
  protected var v1: ExprNode[ExprNodeDesc] = _
  protected var v2: ExprNode[ExprNodeDesc] = _
  
  override def evaluationType() = EvaluationType.SET

  override protected def requireNullValueCheck(parameterIndex: Int) = true

  override protected def cgUDFCall() = {
    getContext().registerImport(classOf[org.apache.hadoop.io.Text])
    getContext().registerImport(classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils])

    ()=> "%s.set(GenericUDFUtils.findText(%s,%s,0) + 1);\n".format(
      UDFInstr.this.resultVariableName(),      v1.valueExpr(),      v2.valueExpr())
  }
  
  override def prepare(rowInspector: ObjectInspector, children: Array[_ <: ExprNode[ExprNodeDesc]]) = {
    if (children.size != 2) {
      throw new CGAssertRuntimeException("expected 2 arguments in the GenericUDFInstr")
    }
    var textOI = ObjectInspectorFactory.getReflectionObjectInspector(
        classOf[Text],
        ObjectInspectorOptions.JAVA)
        
    v1 = ConverterNode(children(0), textOI, ConverterType.HIVE_CONVERTER)
    v2 = ConverterNode(children(1), textOI, ConverterType.HIVE_CONVERTER)

    Array(v1, v2)
  }
}

object UDFInstr {
  def apply(node: GenericFunNode) = new UDFInstr(node)
}

/**
 * between
 * because the "between" generic UDF can be explained as col1 >= col2 and col1 <=col3, we
 * convert it into a new ExprNodeDesc, to reuse the existed code gen snippet class indirectly
 */
object UDFOPBetween {
  def apply(node: GenericFunNode) = {
    val exprs = node.desc.getChildExprs()
    val desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
        ">=",
        exprs.get(1),
        exprs.get(2)),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
        "<=",
        exprs.get(1),
        exprs.get(3))
    ).asInstanceOf[ExprNodeGenericFuncDesc]

    node.getContext().create(desc)
  }
}
