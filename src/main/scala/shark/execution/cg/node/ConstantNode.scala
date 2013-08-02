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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDateObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector
import org.apache.hadoop.io.Text
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.CGContext
import shark.execution.cg.EvaluationType
import shark.execution.cg.ValueType
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBinaryObjectInspector
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.ql.exec.ByteWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.DateWritable

class ConstantNode(context: CGContext, desc: ExprNodeConstantDesc)
  extends ExprNode[ExprNodeConstantDesc](context, desc) {

  private var variableName: String = _
  private var variableType: Class[_] = _
  private var nullValue = false

  private def init(constantValue: AnyRef) {
    var constant = if (nullValue) "" else constantValue.toString() // ConstantWritable
    /**
     * Generate source code for initializing the constant Text value, as byte array
     */
    implicit def textConvert2ByteArrayInHex(writable: Text) =
      if (writable != null)
        "new byte[]{" +
          (for (i <- 0 to writable.getLength() - 1)
            yield "0x" + Integer.toHexString(0xFF & writable.getBytes()(i))).
          reduce(_ + "," + _) +
          "}"
      else
        "null"

    /**
     * Generate source code for initializing the constant BytesWritable value, as byte array
     */
    implicit def byteConvert2ByteArrayInHex(writable: BytesWritable) =
      if (writable != null)
        "new byte[]{" +
          (for (i <- 0 to writable.getLength() - 1)
            yield "0x" + Integer.toHexString(0xFF & writable.getBytes()(i))).
          reduce(_ + "," + _) +
          "}"
      else
        "null"

    /**
     * Generate source code for initializing the constant TimestampWritable value, as 
     * Timestamp
     */        
    implicit def timestampConvert2ByteArrayInHex(writable: TimestampWritable) =
      if (writable != null)
        "new java.sql.Timestamp(" + writable.getTimestamp().getTime() + "l)"
      else
        "null"

    /**
     * Generate source code for initializing the constant DateWritable value, as Date
     */
    implicit def dateConvert2ByteArrayInHex(writable: DateWritable) =
      if (writable != null)
        "new java.sql.Date(" + writable.get().getTime() + "l)"
      else
        "null"

    outputInspector match {
      case x: WritableConstantBooleanObjectInspector => {
        variableName = getVariableName(resultVariableType, constant)
      }
      case x: WritableConstantByteObjectInspector => {
        variableName = getVariableName(resultVariableType, "(byte)" + constant)
      }
      case x: WritableConstantDoubleObjectInspector => {
        variableName = getVariableName(resultVariableType, constant)
      }
      case x: WritableConstantFloatObjectInspector => {
        constant += "f"
        variableName = getVariableName(resultVariableType, constant)
      }
      case x: WritableConstantIntObjectInspector => {
        variableName = getVariableName(resultVariableType, constant)
      }
      case x: WritableConstantLongObjectInspector => {
        constant += "l"
        variableName = getVariableName(resultVariableType, constant)
      }
      case x: WritableConstantShortObjectInspector => {
        variableName = getVariableName(resultVariableType, "(short)" + constant)
      }
      case x: WritableConstantStringObjectInspector => {
        variableName = getVariableName(resultVariableType, constantValue.asInstanceOf[Text])
        constant = resultVariableName
      }
      case x: WritableConstantTimestampObjectInspector => {
        variableName = getVariableName(resultVariableType, 
            constantValue.asInstanceOf[TimestampWritable])
        constant = resultVariableName
      }
      case x: WritableConstantDateObjectInspector => {
        variableName = getVariableName(resultVariableType, 
            constantValue.asInstanceOf[DateWritable])
        constant = resultVariableName
      }
      case x: WritableConstantBinaryObjectInspector => {
        variableName = getVariableName(resultVariableType, 
            constantValue.asInstanceOf[BytesWritable])
        constant = resultVariableName
      }
      case x: WritableVoidObjectInspector => {
        variableName = getContext().createValueVariableName(ValueType.TYPE_VALUE,
          resultVariableType, true, true, false, " = new org.apache.hadoop.io.NullWritable.get()")
        constant = resultVariableName
      }
      case _ => 
        throw new CGAssertRuntimeException("undefined OI [" + outputInspector + "]")
    }
    
    ConstantNode.this.codeEvaluateSnippet = ()=>null
  }

  // create variable with specified type and constructor parameters for gen code
  private def getVariableName(variableType: Class[_], parameter: String) =
    getContext().createValueVariableName(
      ValueType.TYPE_VALUE, 
      variableType,
      !nullValue, // create variable?
      true, // is final?
      false, // is static?
      " = new " + variableType.getCanonicalName() + "(" + parameter + ")") // initializing

  override def prepare(rowInspector: ObjectInspector) = {
    var primitveOutputInspector = castToPrimitiveObjectInspector(desc.getWritableObjectInspector())
    
    if (primitveOutputInspector != null) {
      setOutputInspector(primitveOutputInspector)
      var coi = primitveOutputInspector.asInstanceOf[ConstantObjectInspector]
      var constantValue = coi.getWritableConstantValue()
      variableType = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
                       primitveOutputInspector.getPrimitiveCategory()).primitiveWritableClass

      if (null == constantValue) {
        nullValue = true
      }
      
      init(constantValue)
      true
    } else {
      false
    }
  }
  
  override def evaluationType() = if (nullValue) EvaluationType.NULL else EvaluationType.CONSTANT
  override def resultVariableType() = variableType
  override def resultVariableName() = variableName
  override def constantNull() = nullValue
  override def codeInit(): LinkedHashSet[String] = new LinkedHashSet[String]()
}

object ConstantNode {
  def apply(context: CGContext, desc: ExprNodeConstantDesc) = 
    new ConstantNode(context, desc)
}