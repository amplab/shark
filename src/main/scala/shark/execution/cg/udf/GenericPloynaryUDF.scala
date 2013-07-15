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
import org.apache.hadoop.io.BooleanWritable
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import scala.runtime.ArrayRuntime

/*
 * 1) This file is used to re-implement the generic UDF, any un-recognized UDF will fail the CG and
 * resort to the Hive Expr Eval;
 * 2) The following code could be examples for the new GenericUDF re-implementation
 */

/**
 * printf
 */
class UDFPrintf(node: GenericFunNode) extends UDFCodeGen(node) {

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

/**
 * if
 */
object UDFIf {
  def apply(node: GenericFunNode) = {
    val exprs = node.desc.getChildExprs()
    val desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("case",
        exprs.get(0),
        new ExprNodeConstantDesc(true),
        exprs.get(1),
        exprs.get(2)
    ).asInstanceOf[ExprNodeGenericFuncDesc]

    node.getContext().create(desc)
  }
}

/**
 * when
 * "CASE WHEN a THEN b WHEN c THEN d [ELSE f] END"
 * a and c should be boolean
 */
object UDFWhen {
  def apply(node: GenericFunNode) = {
    import scala.collection.JavaConversions._
    
    val exprs = new ExprNodeConstantDesc(true)::node.desc.getChildExprs().toList
    var sss = java.util.Arrays.asList(exprs:_*)
    val desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("case", 
        exprs.toList:_*
    ).asInstanceOf[ExprNodeGenericFuncDesc]

    node.getContext().create(desc)
  }
}

/**
 * Case
 */
class UDFCase(node: GenericFunNode) 
  extends UDFCodeGen(node) {
  
  override def evaluationType() = EvaluationType.GET

  override protected def requireNullValueCheck(parameterIndex: Int) = true

  /**
   * case value when A then a [when B then b] else c end
   * if value == null then the value would be "c", if any, other wise null
   */
  override protected def cgInnersDeferred(
    args: Array[_ <: ExprCodeGen],
    parameterIdx: Int,
    udfCall: String,
    partialEvaluate: Boolean): StringBuffer = {
    var content = new StringBuffer()
    if (parameterIdx >= arguments.length) {
      content.append(udfCall + "\n")
    } else {
      var node_0 = arguments(0)
      var node   = arguments(parameterIdx)
      addEvaluateCode(content, node)
      
      if (parameterIdx == 0) {
        // CASE
        var condition = concatExprs("&&", node.cgValidateCheck(), null)
        if (condition != null) {
          content.append(
          "if(%s){\n %s\n} else {\n  %s\n}\n".format(condition, 
             cgInnersDeferred(arguments, 1, udfCall, true),
             // try to jump to the END value
             cgInnersDeferred(
                 arguments, arguments.length - (arguments.length + 1) % 2, 
                 udfCall, 
                 true))
           )
        } else {
          content.append(cgInnersDeferred(arguments, 1, udfCall, true))
        }
      } else if (parameterIdx == arguments.length - 1 || parameterIdx % 2 == 0) { 
        // ELSE or THEN
          content.append(cgUDFCall(parameterIdx))
      } else { 
        // WHEN
        var condition = node.cgValidateCheck()
          condition = concatExprs("&&", 
              condition, 
              "%s.equals(%s)".format(node_0.resultVariableName(),node.resultVariableName()))

        content.append(
          "if(%s){\n %s\n} else {\n  %s\n}\n".format(condition, 
              cgInnersDeferred(arguments, parameterIdx + 1, udfCall, true), // jump to THEN 
              cgInnersDeferred(arguments, parameterIdx + 2, udfCall, true)) // jump to next WHEN
        )
      }
      
      if (partialEvaluate) {
        for(i <- parameterIdx until args.length) {
          args(i).notifyEvaluatingCodeGenNeed()
          args(i).notifycgValidateCheckCodeNeed()
        }
      }
    }
    
    content
  }
    
  private def cgUDFCall(idx: Int) = {
    var valueNode = arguments(idx)
    var cgValidate = valueNode.cgValidateCheck()
    
    if (cgValidate != null) {
      "if(%s) {\n  %s = %s;\n} else {\n  %s;\n}".format(cgValidate, 
          this.resultVariableName(), 
          valueNode.valueExpr(),
          invalidValueExpr())
    } else {
      "%s = %s;\n".format(this.resultVariableName(), valueNode.valueExpr())
    }
  }
  
  override protected def cgUDFCall() = {
    // if can not find any matches
    if (arguments.length % 2 == 0) {
      // has the "ELSE"
      ()=> cgUDFCall(arguments.length - 1)
    } else {
      ()=>{
        var expr = invalidValueExpr()
        if (null != expr) 
          expr+ ";"
        else
          ""
      }
    }
  }
  
  override def prepare(rowInspector: ObjectInspector, children: Array[_ <: ExprNode[ExprNodeDesc]]):
    Array[_<:ExprNode[ExprNodeDesc]] = {
    var caseOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true)
    var returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true)
    var subnodes = ArrayBuffer[ExprNode[ExprNodeDesc]]()
    
    var childrenDesc = node.desc.getChildExprs()
    if (childrenDesc.size() != children.length) {
      // can not handle
      return null
    }
    
    for(i <- 0 until childrenDesc.size()) {
      if (i == 0 || (i % 2 == 1 && i != childrenDesc.size() - 1)) {
        if (!caseOIResolver.update(childrenDesc.get(i).getWritableObjectInspector()))
          throw new UDFArgumentTypeException(i + 1,
            "The expressions after WHEN should have the same type with that after CASE: \""
            + caseOIResolver.get().getTypeName() + "\" is expected but \""
            + childrenDesc.get(i).getTypeString() + "\" is found")
      } else {
        if (!returnOIResolver.update(childrenDesc.get(i).getWritableObjectInspector())) {
          throw new UDFArgumentTypeException(i + 1,
            "The expressions after THEN should have the same type: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + childrenDesc.get(i).getTypeString()
            + "\" is found")
        }
      }
    }
    
    for(i <- 0 until children.length) {
      if (i == 0 || (i % 2 == 1 && i != children.length - 1))
        subnodes.+=(ConverterNode(children(i), caseOIResolver.get(), ConverterType.HIVE_CONVERTER))
      else
        subnodes.+=(ConverterNode(children(i), returnOIResolver.get(), ConverterType.HIVE_CONVERTER))
    }
    
    assert(this.getOutputInspector() == returnOIResolver.get())

    subnodes.toArray
  }
}

object UDFCase {
  def apply(node: GenericFunNode) = new UDFCase(node)
}
