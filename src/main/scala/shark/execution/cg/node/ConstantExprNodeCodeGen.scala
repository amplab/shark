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

package shark.execution.cg.node

import scala.collection.mutable.LinkedHashSet

import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantTimestampObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector
import org.apache.hadoop.io.Text

import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.CGContext
import shark.execution.cg.EvaluationType
import shark.execution.cg.ValueType

class ConstantExprNodeCodeGen(context : CGContext, desc : ExprNodeConstantDesc) 
  extends ExprNodeCodeGen[ExprNodeConstantDesc](context, desc) {
  private var outputOI = if (getDesc() == null) null else getDesc().getWritableObjectInspector()
  private var isConstantNull : Boolean = false
  private var variableName : String = _
  private var variableType : Class[_] = _

  def this(context : CGContext, outputOI : ConstantObjectInspector) {
    this(context, null : ExprNodeConstantDesc)
    this.outputOI = outputOI
  }

  override def constantNull() = isConstantNull

  private def initWithNullConstant() {
    isConstantNull = true

    outputOI match {
//      case x : StandardConstantListObjectInspector => {/*TODO*/ setCodeEvaluate("null")}
//      case x : StandardConstantMapObjectInspector => {/*TODO*/ setCodeEvaluate("null") }
      case x : WritableConstantBooleanObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.io.BooleanWritable])) 
      case x : WritableConstantByteObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.hive.serde2.io.ByteWritable]))
      case x : WritableConstantDoubleObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable]))
      case x : WritableConstantFloatObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.io.FloatWritable]))
      case x : WritableConstantIntObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.io.IntWritable]))
      case x : WritableConstantLongObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.io.LongWritable]))
      case x : WritableConstantShortObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.hive.serde2.io.ShortWritable]))
      case x : WritableConstantStringObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.io.Text]))
      case x : WritableConstantTimestampObjectInspector => setCodeEvaluate(
          getVariableNameWithNull(classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable]))
      case x : WritableVoidObjectInspector => setCodeEvaluate(
          getContext().createValueVariableName(ValueType.TYPE_VALUE, 
              classOf[org.apache.hadoop.io.NullWritable], true, true, false, " = null"))
      case _ => throw new 
          CGAssertRuntimeException("undefined OI [" + outputOI.getClass().getCanonicalName() + "]")
    }
  }

  private def initWithNonNullConstant(constantValue : AnyRef) {
    var constant : String = constantValue.toString()
    /*convert the Text value as Text variable in the generated code */
    implicit def convert2ByteArrayInHex(text : Text) = 
      "new byte[]{" +
      ((for (i <- 0 to text.getLength() - 1) yield "0x" + Integer.toHexString(0xFF & text.getBytes()(i))).reduceLeft(_+","+_)) + 
      "}"
    
    outputOI match {
      case x : StandardConstantListObjectInspector => {/*TODO*/}
      case x : StandardConstantMapObjectInspector => {/*TODO*/}
      case x : WritableConstantBooleanObjectInspector => {
        variableType = classOf[org.apache.hadoop.io.BooleanWritable]
        variableName = getVariableName(resultVariableType, constant)
      }
      case x : WritableConstantByteObjectInspector => {
        variableType = classOf[org.apache.hadoop.hive.serde2.io.ByteWritable]
        variableName = getVariableName(resultVariableType, "(byte)" + constant)
      }
      case x : WritableConstantDoubleObjectInspector => {
        variableType = classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable]
        variableName = getVariableName(resultVariableType, constant)
      }
      case x : WritableConstantFloatObjectInspector => {
        constant += "f"
        variableType = classOf[org.apache.hadoop.io.FloatWritable]
        variableName = getVariableName(resultVariableType, constant)
      }
      case x : WritableConstantIntObjectInspector => {
        variableType = classOf[org.apache.hadoop.io.IntWritable]
        variableName = getVariableName(resultVariableType, constant)
      }
      case x : WritableConstantLongObjectInspector => {
        constant += "l"
        variableType = classOf[org.apache.hadoop.io.LongWritable]
        variableName = getVariableName(resultVariableType, constant)
      }
      case x : WritableConstantShortObjectInspector => {
        variableType = classOf[org.apache.hadoop.hive.serde2.io.ShortWritable]
        variableName = getVariableName(resultVariableType, "(short)" + constant)
      }
      case x : WritableConstantStringObjectInspector => {
        variableType = classOf[org.apache.hadoop.io.Text]
        variableName = getVariableName(resultVariableType, constantValue.asInstanceOf[Text])
        constant = resultVariableName
      }
      case x : WritableConstantTimestampObjectInspector => {
        variableType = classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable]
        variableName = getVariableName(resultVariableType, "new java.sql.Timestamp(" + 
            (outputOI.asInstanceOf[WritableConstantTimestampObjectInspector]).
              getWritableConstantValue().getTimestamp().getTime() + "l)")
        constant = resultVariableName
      }
      case x : WritableVoidObjectInspector => {
        variableType = classOf[org.apache.hadoop.io.NullWritable]
        variableName = getContext().createValueVariableName(ValueType.TYPE_VALUE, 
            resultVariableType, true, true, false, " = new org.apache.hadoop.io.NullWritable.get()")
        constant = resultVariableName
      }
      case _ => throw new 
          CGAssertRuntimeException("undefined OI [" + outputOI.getClass().getCanonicalName() + "]")
    }
    this.setCodeEvaluate(constant);
  }

  private def init() : ConstantObjectInspector = {
    if (!(outputOI.isInstanceOf[ConstantObjectInspector] && outputOI.isInstanceOf[PrimitiveObjectInspector])) {
      // currently doesn't support the Map/List
      return null
    }

    var constantValue : AnyRef = (outputOI.asInstanceOf[ConstantObjectInspector]).getWritableConstantValue()

    if (null != constantValue) {
      initWithNonNullConstant(constantValue)
    } else {
      initWithNullConstant()
    }

    outputOI
  }

  private def getVariableName(variableType : Class[_], parameter : String) =
    getContext().createValueVariableName(ValueType.TYPE_VALUE, variableType, 
        true, true, false, " = new " + variableType.getCanonicalName() + "(" + parameter + ")");

  private def getVariableNameWithNull(variableType : Class[_]) =
    getContext().createValueVariableName(ValueType.TYPE_VALUE, variableType, true, true, false, " = null")

  override def evaluationType() = EvaluationType.CONSTANT
  override def create(oi : ObjectInspector) = init()
  override def resultVariableType() = variableType
  override def resultVariableName() = variableName
  override def codeInit() : LinkedHashSet[String] = new LinkedHashSet[String]()
  
  override def valueInJavaType() : String = valueInJava
  private lazy val valueInJava:String = {
    var value = super.valueInJavaType()
    if("null".equals(value)) 
      "null"
    else 
      getContext().createValueVariableName(ValueType.TYPE_VALUE,
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveWritableClass(resultVariableType()).primitiveJavaClass, 
          true, 
          true, 
          false, 
          " = " + value) 
  }
}