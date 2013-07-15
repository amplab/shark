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

import java.lang.reflect.Type
import java.lang.reflect.Field

import scala.collection.mutable.LinkedHashSet

import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.`lazy`.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo

import shark.execution.cg.node.GenericFunNode
import shark.execution.cg.node.ExprNode
import shark.execution.cg.EvaluationType
import shark.execution.cg.ExprCodeGen
import shark.execution.cg.ValueType
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.node.ConverterNode


abstract class UDFCodeGen(val node: GenericFunNode)
  extends ExprCodeGen(node.getContext()) {

  private lazy val codeUDFCall = cgUDFCall()
  protected var arguments:Array[_<:ExprNode[ExprNodeDesc]] = node.children
  private var constantNullCheck = ()=> 
    (for (ele <- this.arguments.zipWithIndex) 
      yield (this.requireNullValueCheck(ele._2) && ele._1.constantNull())).
    foldLeft(codeUDFCall == null)(_ || _)

  // null value indicator, which may not necessary for some case(EvaluationType.GET)
  private lazy val nullIndicator = getContext().createNullIndicatorVariableName()
  private lazy val variableName = getContext().createValueVariableName(
      ValueType.TYPE_VALUE, 
      resultVariableType(), 
      // create instance?
      evaluationType() != EvaluationType.GET,
      // is final?
      evaluationType() != EvaluationType.GET,
      false,
      null)
  
  /**
   * helper function to add the evaluating code into StringBuffer
   * 
   */
  protected def addEvaluateCode(content: StringBuffer, node: ExprCodeGen) {
    var evaluate = node.cgEvaluate()
    if (evaluate != null) {
      content.append(evaluate + "\n")
    }
  }
    
  protected def concatExprs(op: String, v1: String, v2: String) = {
    if (v1 == null && v2 != null) v2
    else if (v2 == null && v1 != null) v1
    else if (v1 == null && v2 == null) null
    else v1 + op + v2
  }
  
  private def addCodeSnippet(content: StringBuffer, expr: String) {
    if (null != expr) {
      content.append(expr + ";\n")
    }
  }  

  /**
   * to mark the current UDF as null value
   */
  protected def markAsConstantNull(result: Boolean = true) {
    constantNullCheck = ()=>result
  }
  
  override def nullValueIndicatorVariableName() = nullIndicator

  /**
   * Generate the code for value check/evaluating in eager / deferred;
   * @param args arguments of current UDF (/generic UDF)
   * @param parameterIdx
   * @param udfCall
   */
  protected def cgInners(args: Array[_<:ExprCodeGen]):String = {
    var code: StringBuffer = new StringBuffer()
    var isConstantNull = this.constantNull()
    if (isConstantNull) {
      addCodeSnippet(code, invalidValueExpr())
    } else {
      addCodeSnippet(code, initValueExpr())
    }

    // get the udf call string
    var udfCall = if (codeUDFCall != null) codeUDFCall() else ""
    
    args.foreach(node=>if (node.isStateful()) addEvaluateCode(code, node))
    if (!isConstantNull || FunctionRegistry.isStateful(node.genericUDF)) {
      code.append(cgInnersDeferred(args, 0, udfCall, false))
    }
    
    code.toString()
  }
  
  /**
   * return if the Nth parameters requires the null value check
   * all of the existed UDF functions will not require the null
   * value check for the parameter, as they will handle that internally.
   *
   * But that's not true for the re-implemented UDF function.
   *
   * The function should be consistent of return value with 1 or more invokes.
   * @param parameterIndex
   * @return true for require the parameter null value check, otherwise false;
   */
  protected def requireNullValueCheck(parameterIndex: Int) = false

  /**
   * Gen code for evaluating the UDF value, by checking the validity of its children nodes 1 by 1,
   * and the it will return the value immediately if child node(parameter) is:
   * 1) with null value, but current UDF requires the parameter non-null
   *    e.g. in expr "a + b", if "a" is null, then the expr value is null without considering 
   *       the value of "b"
   * 2) the value is sufficient for evaluating, without considering the next parameter(s)
   *    e.g. in expr "a or b", if "a" is true, then the expr value is true, without considering
   *       the value of "b"
   * Both of cases will optimize the performance of expr evaluating
   */
  protected def cgInnersDeferred(
    args: Array[_<:ExprCodeGen],
    parameterIdx: Int,
    udfCall: String,
    partialEvaluate: Boolean): StringBuffer = {
    var content = new StringBuffer()
    
    if (parameterIdx >= args.length) {
      content.append(udfCall + "\n")
    } else {
      addEvaluateCode(content, args(parameterIdx))
  
      var partial = shortcut(parameterIdx)
  
      if (partial != null) {
        assert(partial._2 != null)
        if(partial._1 == null) {
          content.append(cgInnersDeferred(args, parameterIdx + 1, udfCall, true))
        } else {
          content.append(
          "if (%s) {\n%s\n} else {\n%s\n}\n".format(
              partial._1, 
              if(partial._3) partial._2 else cgInnersDeferred(args, parameterIdx + 1, udfCall, true),
              if(partial._3) cgInnersDeferred(args, parameterIdx + 1, udfCall, true) else partial._2)
           )
        }
      } else {
        content.append(cgInnersDeferred(args, parameterIdx + 1, udfCall, false))
      }
  
      if (partialEvaluate) {
        // since the "else" branch may not be executed, then
        // we need to re-generate the evaluating code again for the
        // impacted[current] node in the future;
        for(i <- parameterIdx until args.length) {
          args(i).notifyEvaluatingCodeGenNeed()
          args(i).notifycgValidateCheckCodeNeed()
        }
      }
    }
    
    content
  }

  // tuple (condition, short cut evaluation expression, if the expression in IF branch)
  protected def shortcut(currentParamIdx: Int): (String, String, Boolean) = {
    var condition: String = null

    if (requireNullValueCheck(currentParamIdx)) {
      var validation = arguments(currentParamIdx).cgValidateCheck()
      if (validation != null) arguments(currentParamIdx).notifycgValidateCheckCodeNeed()
      
      (validation, invalidValueExpr() + ";", false)
    } else 
      null
  }

  /**
   * code snippet of the UDF implementation, the sub class has to override this function 
   */
  protected def cgUDFCall(): ()=>String

  override def evaluationType() = EvaluationType.SET

  /**
   *if exist the UDF parameter, which requires the null value check, and it's constant null, 
   *that means the value of UDF is also null 
   */
  final override def constantNull() = constantNullCheck()
    
  // combine the code snippet of inside the method of "init(ObjectInspector oi)", the code snippet
  // sequence should be well cared
  override def codeInit() = arguments.foldLeft(LinkedHashSet[String]())(_ | _.codeInit())
  
  final override def fold(rowInspector: ObjectInspector): Boolean = {
    setOutputInspector(node.getOutputInspector())
    
    arguments = prepare(rowInspector, arguments)
    if (arguments == null) {
      false
    } else {
      codeWritableNameSnippet = ()=>variableName
      if(arguments.length != 0) {
        arguments.foldLeft(true)(_&&_.fold(rowInspector))
      } else {
        true
      }
    }
  }
  
  /**
   * Sub class should decide to make the arguments as ConvertNode(different types of) if necessary
   * @return null for the UDF CodeGen may not able to handle the of its arguments, otherwise
   *              the wrapped (as ConvertNode) arguments. 
   */
  protected def prepare(
      rowInspector: ObjectInspector, 
      children: Array[_<:ExprNode[ExprNodeDesc]]): 
    Array[_<:ExprNode[ExprNodeDesc]]
  
  override def notifyEvaluatingCodeGenNeed() {
    super.notifyEvaluatingCodeGenNeed()
    for (child <- arguments) child.notifyEvaluatingCodeGenNeed()
  }

  override def cgEvaluate() = cgInners(this.arguments)
}