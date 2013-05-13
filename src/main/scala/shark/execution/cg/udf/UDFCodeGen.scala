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
import scala.collection.JavaConversions.collectionAsScalaIterable
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo

import shark.execution.cg.node.GenericFunExprNodeCodeGen
import shark.execution.cg.node.ExprNodeCodeGen
import shark.execution.cg.EvaluationType
import shark.execution.cg.ExprCodeGen
import shark.execution.cg.ValueType

abstract class UDFCodeGen(val node : GenericFunExprNodeCodeGen) 
  extends ExprCodeGen(node.getContext()) {
    
  protected var arguments = node.getChildren()
  
  // result type
  private lazy val typeWritable = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
    (node.outputOI.asInstanceOf[PrimitiveObjectInspector]).getPrimitiveCategory()).primitiveWritableClass

  private lazy val variableName = 
    getContext().createValueVariableName(ValueType.TYPE_VALUE, typeWritable, evaluationType() != EvaluationType.GET)
  private lazy val nullIndicator = getContext().createNullIndicatorVariableName()
  
  override def nullValueIndicatorVariableName() = nullIndicator

  /**
   * Generate the code for value check/evaluating in eager / deferred;
   * @param args arguments of current UDF (/generic UDF)
   * @param parameterIdx
   * @param udfCall
   */
  protected def cgInners(args : Array[ExprNodeCodeGen[ExprNodeDesc]], udfCall : String) = {
    var content = new StringBuffer
    var initValue = initValueExpr()
    if (initValue != null) {
      content.append(initValue + ";\n")
    }

    var constantNull = this.constantNull()

    if (this.node.isStateful()) {
      cgInnersEager(content, args, udfCall, constantNull)
    } else {
      if (!constantNull) {
        cgInnersDeferred(content, args, 0, udfCall, false)
      }
    }
    
    content.toString()
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
  protected def requireNullValueCheck(parameterIndex : Int) = false

  protected def cgInnersDeferred(content : StringBuffer, args : Array[ExprNodeCodeGen[ExprNodeDesc]], parameterIdx : Int, udfCall : String, partialEvaluate : Boolean) {
    if (parameterIdx == args.length) {
      content.append(udfCall + "\n")
      return
    }

    if (args(parameterIdx).evaluationType() != EvaluationType.CONSTANT) {
      var evaluate = args(parameterIdx).codeEvaluate()
      if (evaluate != null) {
        content.append(evaluate + "\n")
      }
    }
    var checkValid:String = null

    if (requireNullValueCheck(parameterIdx)) {
      checkValid = args(parameterIdx).cgValidateCheck()
      if(null != checkValid) {
        content.append("if(" + checkValid + ") {\n")
      }
    }

    var checkEvaluate = partialEvaluateCheck(args, parameterIdx)
    var resultEvaluate = partialEvaluateResult(args, parameterIdx)

    var bePartialEvaluate = ((checkEvaluate != null) && (resultEvaluate != null))

    if (bePartialEvaluate) {
      content.append("if (" + checkEvaluate + ") { \n")
      content.append(resultEvaluate + ";\n")
      content.append("} else {\n")
    }

    cgInnersDeferred(content, args, parameterIdx + 1, udfCall, bePartialEvaluate)

    if (bePartialEvaluate) {
      content.append("}\n")
    }

    if (requireNullValueCheck(parameterIdx) && null != checkValid) {
      var expr = invalidValueExpr()
      content.append("} else {\n")
      if (null != expr) {
        content.append(expr + ";\n")
      }
      content.append("}\n")
    }

    if (partialEvaluate) {
      // since the "else" branch may not be executed, then
      // we need to re-generate the execute code again for the
      // impacted[current] node in the future;
      args(parameterIdx).notifyEvaluatingCodeGenNeed()
    }
  }

  /**
   * the function is used for the partial evaluating, like in "or" / "and" etc. which will optimize the performance
   * @param content
   * @param args
   * @param parameterIdx
   */
  protected def partialEvaluateCheck(args : Array[ExprNodeCodeGen[ExprNodeDesc]], currentParamIdx : Int) : String = null

  protected def partialEvaluateResult(args : Array[ExprNodeCodeGen[ExprNodeDesc]], currentParamIdx : Int) : String = null

  protected def cgInnersEager(content : StringBuffer, args : Array[ExprNodeCodeGen[ExprNodeDesc]], udfCall : String, constantNull : Boolean) {
    for (i <- 0 until args.length) {
      if (args(i).evaluationType() != EvaluationType.CONSTANT) {
        var evaluate = args(i).codeEvaluate()
        if (evaluate != null) {
          content.append(evaluate + "\n")
        }
      }
    }

    if (constantNull) {
      // no need for further computing.
      return ;
    }

    var checkValidExprs : Array[String] = new Array[String](args.length)
    for (i <- 0 until args.length) {
      checkValidExprs(i) = args(i).cgValidateCheck();

      if (requireNullValueCheck(i) && null != checkValidExprs(i)) {
        content.append("if(" + checkValidExprs(i) + ") {\n")
      }
    }

    content.append(udfCall + "\n")

    for (i <- 0 until args.length) {
      if (requireNullValueCheck(i) && null != checkValidExprs(i)) {
        var expr = invalidValueExpr()
        content.append("} else {\n")
        if (null != expr) {
          content.append(expr + ";\n")
        }
        content.append("}\n")
      }
    }
  }

  protected def cgUDFCall(args : Array[ExprNodeCodeGen[ExprNodeDesc]]):String

  protected def getExprValue(node : ExprNodeCodeGen[ExprNodeDesc]) = {
    if (node.evaluationType() == EvaluationType.CONSTANT)
      node.codeEvaluate()
    else
      node.resultVariableName() + ".get()"
  }

  override def evaluationType() = EvaluationType.SET
  override def initValueExpr() = nullValueIndicatorVariableName() + "=true"
  override def invalidValueExpr() = nullValueIndicatorVariableName() + "=false"
  override def constantNull() = (for (ele <- this.arguments.zipWithIndex) yield (this.requireNullValueCheck(ele._2) && ele._1.constantNull())).foldLeft(false)(_||_)
  override def codeInit() = arguments.foldLeft(LinkedHashSet[String]())(_ | _.codeInit())
  override def resultVariableType() = this.typeWritable
  override def initialize(rowInspector : ObjectInspector) : ObjectInspector = this.node.outputOI
  override def resultVariableName() = variableName

  override def codeEvaluate() = cgInners(this.arguments, cgUDFCall(this.arguments))
  
  protected def extractOutputObjectInspector(inputOI : ObjectInspector, outputType : Type) = {
    if (outputType == classOf[Object])
      ObjectInspectorUtils
        .getStandardObjectInspector(inputOI, ObjectInspectorCopyOption.JAVA)
    else
      ObjectInspectorFactory
        .getReflectionObjectInspector(outputType, ObjectInspectorOptions.JAVA)
  }
  
  /* the parameter type converting, which might be used in its subclasses*/
  protected var converterNames : Array[String] = _
  protected var outputTypes : Array[String] = _
  
  protected def createConverter(input : PrimitiveObjectInspector, output : PrimitiveObjectInspector, argIndex:Int) {
    outputTypes(argIndex) = null
    converterNames(argIndex) = null
    
    if (output.getPrimitiveCategory() != input.getPrimitiveCategory()) {
        var converterType = classOf[org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter]
        var initStr = 
          "= org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter(" +
          "org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(" +
          "org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory." + input.getPrimitiveCategory().name() + ")," +
          "org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(" +
          "org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory." + output.getPrimitiveCategory().name() + "))"

        outputTypes(argIndex) = output.getPrimitiveWritableClass().getCanonicalName()
        converterNames(argIndex) = if (input == output) null else getContext().createValueVariableName(ValueType.TYPE_CONVERTER, converterType, true, true, false, initStr)
    } 
  }
  
  protected def initialConverters(inputs:Array[ObjectInspector], outputs:Array[ObjectInspector]) : Boolean = {
    converterNames = new Array[String](outputs.length)
    outputTypes = new Array[String](outputs.length)

    for (i <- 0 until outputs.length) {
      if (!(inputs(i).isInstanceOf[PrimitiveObjectInspector] && outputs(i).isInstanceOf[PrimitiveObjectInspector])) {
        // TODO currently only support the primitive object inspector;
        return false
      }

      var output : PrimitiveObjectInspector = outputs(i).asInstanceOf[PrimitiveObjectInspector]
      var input : PrimitiveObjectInspector = inputs(i).asInstanceOf[PrimitiveObjectInspector]

      createConverter(input, output, i)
    }
    
    true
  }
  
  protected def convert(outputType:String, converterName:String, subVariableName:String) = {
    if (null == converterName) 
      subVariableName 
    else 
      "(%s)(%s.convert(%s))".format(outputType, converterName, subVariableName)
  }
}