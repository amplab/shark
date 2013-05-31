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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector

import shark.execution.cg.node.GenericFunExprNodeCodeGen
import shark.execution.cg.node.ExprNodeCodeGen
import shark.execution.cg.ValueType
import shark.execution.cg.EvaluationType

class BridgeUDFCodeGen(udf : GenericUDFBridge, node : GenericFunExprNodeCodeGen) 
  extends UDFCodeGen(node) {

  protected lazy val udfVariableName : String = getContext().createValueVariableName(ValueType.TYPE_UDF, udf.getUdfClass(), true, true, false, null)
  
  override def initValueExpr() = null // this.resultVariableName() + "=null"
  override def invalidValueExpr() = null
  override def evaluationType() = EvaluationType.GET
  
  override protected def cgUDFCall(args : Array[ExprNodeCodeGen[ExprNodeDesc]]) = {
    var zips = for (ele <- args.zip(converterNames).zip(outputTypes)) 
      yield convert(ele._2, ele._1._2, ele._1._1.resultVariableName())

    var udfCall = resultVariableName() + "=" + udfVariableName + ".evaluate(";
    if (zips.length > 0) {
      udfCall += zips.reduceLeft(_ + "," + _)
    }

    udfCall += ");\n"

    udfCall
  }

  override def initialize(rowInspector : ObjectInspector) : ObjectInspector = {
    var f1 = classOf[GenericUDFBridge].getDeclaredField("conversionHelper")
    var f3 = classOf[ConversionHelper].getDeclaredField("givenParameterOIs")
    var f4 = classOf[ConversionHelper].getDeclaredField("methodParameterTypes")

    f1.setAccessible(true)
    f3.setAccessible(true)
    f4.setAccessible(true)

    var helper = f1.get(udf).asInstanceOf[ConversionHelper]
    var isVariableLengthArgument = false
    var givenParameterOIs = f3.get(helper).asInstanceOf[Array[ObjectInspector]]
    var methodParameterTypes = f4.get(helper).asInstanceOf[Array[Type]]

    var lastParaElementType : Type = null
    if (methodParameterTypes.length > 0) {
      lastParaElementType = TypeInfoUtils.getArrayElementType(methodParameterTypes(methodParameterTypes.length - 1))
      if (null == lastParaElementType) {
        // not array;
        lastParaElementType = methodParameterTypes(methodParameterTypes.length - 1)
      } else {
        isVariableLengthArgument = true
      }
    }

    if (isVariableLengthArgument) {
      if (methodParameterTypes.length > givenParameterOIs.length) {
        // UDF function signature doesn't match the input parameters;
        return null
      }
    } else {
      if (methodParameterTypes.length != givenParameterOIs.length) {
        // UDF function signature doesn't match the input parameters;
        return null
      }
    }

    // Create the output OI array
    var methodParameterOIs = for (ele <- givenParameterOIs.zipWithIndex) yield extractOutputObjectInspector(ele._1,
      if (ele._2 >= methodParameterTypes.length - 1 && lastParaElementType != null)
        lastParaElementType
      else
        methodParameterTypes(ele._2))

    converterNames = new Array[String](methodParameterOIs.length)
    outputTypes = new Array[String](methodParameterOIs.length)

    initialConverters(givenParameterOIs, methodParameterOIs)

    this.node.outputOI
  }
}