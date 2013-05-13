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

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import shark.execution.cg.node.GenericFunExprNodeCodeGen
import shark.execution.cg.ValueType
import shark.execution.cg.node.ExprNodeCodeGen
import shark.execution.cg.EvaluationType

class GenericUDFPrintfCodeGen(node : GenericFunExprNodeCodeGen) extends UDFCodeGen(node) {
  override def evaluationType() = EvaluationType.SET
  
  override protected def cgUDFCall(nodes : Array[ExprNodeCodeGen[ExprNodeDesc]]) = {
    var formatterVariable = 
      getContext().createValueVariableName(ValueType.TYPE_VALUE, classOf[java.util.Formatter], false)
    ("%s = new java.util.Formatter(java.util.Locale.US);\n" + 
    "%s.format(%s);\n" +
    "%s.set(%s.toString());\n").format(
    formatterVariable,
    formatterVariable, ((for (ele <- nodes) yield ele.valueInJavaType()).reduceLeft(_+","+_)),
    resultVariableName(), formatterVariable)
  }
  
  override protected def requireNullValueCheck(parameterIndex : Int) = if (0 == parameterIndex) true else false
}

class GenericUDFInstrCodeGen (node : GenericFunExprNodeCodeGen) extends UDFCodeGen(node) {
  override protected def requireNullValueCheck(parameterIndex : Int) = true
  override def evaluationType() = EvaluationType.SET
  override protected def cgUDFCall(nodes : Array[ExprNodeCodeGen[ExprNodeDesc]]) = {
    "%s.set(org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.findText(%s,%s,0) + 1);\n".format(
       this.resultVariableName(),
       if (converterNames(0) == null) 
         nodes(0).resultVariableName() 
       else 
         "(org.apache.hadoop.io.Text)%s.convert(%s)".format(converterNames(0), nodes(0).resultVariableName()),
       if (converterNames(1) == null) 
         nodes(1).resultVariableName() 
       else 
         "(org.apache.hadoop.io.Text)%s.convert(%s)".format(converterNames(1), nodes(1).resultVariableName())
    )
  }
  
  override def initialize(rowInspector : ObjectInspector) : ObjectInspector = {
    if(initialConverters(
        this.arguments.map(_.getOutputOI()), 
        Array[ObjectInspector](PrimitiveObjectInspectorFactory.writableStringObjectInspector, 
                               PrimitiveObjectInspectorFactory.writableStringObjectInspector))
       && this.converterNames.length == 2 && outputTypes.length == 2) {
      super.initialize(rowInspector)
    } else {
      logWarning("Can't initialize the converters")
      null
    }
  }
}