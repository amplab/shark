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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import shark.execution.cg.node.GenericFunNode
import shark.execution.cg.node.ExprNode
import shark.execution.cg.ValueType
import shark.execution.cg.EvaluationType
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.ExprCodeGen
import shark.execution.cg.node.ConverterNode
import shark.execution.cg.node.ConverterType

/**
 * The class is bridge to the existed UDFs (not re-implemented)
 */
case class BridgeUDF(udf: GenericUDFBridge, override val node: GenericFunNode)
  extends UDFCodeGen(node) {

  protected lazy val udfVariableName: String = 
    getContext().createValueVariableName( // create the UDF object
        ValueType.TYPE_UDF, 
        udf.getUdfClass(), 
        true, 
        true, 
        false, 
        null)

  override def evaluationType() = EvaluationType.GET

  override protected def cgUDFCall() = {
    // resultVariable = udfVariable.evaluate(resultVariableName1, resultVariableName2, ...)
    var udfCall = "%s=%s.evaluate(%s);\n".
                    format(
                      resultVariableName(), 
                      udfVariableName,
                      if (arguments.length == 0)
                        ""
                      else
                        arguments.map(_.valueExpr()).reduce(_ + "," + _)
                    )

    ()=> udfCall
  }

  override def prepare(rowInspector: ObjectInspector, 
      children: Array[_<:ExprNode[ExprNodeDesc]]): Array[_<:ExprNode[ExprNodeDesc]] = {
    /*please refs org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
     * We need to figure out which UDF function(which may has several versions of "evaluate" method) 
     * is the most suitable per the function parameter list, and create the converter(s) if
     * necessary.
     * */
    var f1 = classOf[GenericUDFBridge].getDeclaredField("conversionHelper")
    var f4 = classOf[ConversionHelper].getDeclaredField("methodParameterTypes")

    f1.setAccessible(true)
    f4.setAccessible(true)

    var helper = f1.get(udf).asInstanceOf[ConversionHelper]
    var isVariableLengthArgument = false  // e.g concat() is with variable parameters
    // expected parameters OI (types)
    var methodParameterTypes = f4.get(helper).asInstanceOf[Array[Type]]

    // the last parameter also can be an array, the last parameter of UDF is the Array type
    // if it accept the variable length of parameters (e.g. UDFConcat)
    var lastParaElementType: Type = null
    if (methodParameterTypes.length > 0) {
      lastParaElementType = TypeInfoUtils.getArrayElementType(
          methodParameterTypes(methodParameterTypes.length - 1))
      if (null == lastParaElementType) {
        // not array;
        lastParaElementType = methodParameterTypes(methodParameterTypes.length - 1)
      } else {
        isVariableLengthArgument = true
      }
    }

    if (isVariableLengthArgument) {
      if (methodParameterTypes.length > children.length) {
        // UDF function signature doesn't match the input parameters;
        return null
      }
    } else {
      if (methodParameterTypes.length != children.length) {
        // UDF function signature doesn't match the input parameters;
        return null
      }
    }

    // get the output OI array
    var outputs = 
      for (ele <- children.zipWithIndex) 
        yield extractOutputObjectInspector(ele._1.getOutputInspector(),
          if (ele._2 >= methodParameterTypes.length - 1 && lastParaElementType != null)
            lastParaElementType
          else
            methodParameterTypes(ele._2)
        )
    arguments = for(e <- children.zip(outputs)) yield 
      ConverterNode(e._1, e._2, ConverterType.HIVE_CONVERTER)
    
    arguments
  }
  
   /**
   * get the output ObjectInspector as PrimitiveJavaObjectInspector
   */
  private def extractOutputObjectInspector(inputOI: ObjectInspector, outputType: Type) = {
    if (outputType == classOf[Object])
      ObjectInspectorUtils
        .getStandardObjectInspector(inputOI, ObjectInspectorCopyOption.JAVA)
    else
      ObjectInspectorFactory
        .getReflectionObjectInspector(outputType, ObjectInspectorOptions.JAVA)
  }
}