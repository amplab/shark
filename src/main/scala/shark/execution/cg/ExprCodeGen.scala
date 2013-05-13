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

import java.sql.Timestamp

import scala.collection.mutable.LinkedHashSet
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.LogHelper


/**
 * VarType represent the variable type in the generated code
 * Usually there are 3 types
 * SET: indicate the node value is retrieved from the re-implemented UDF/GenericUDF, 
 *      need to provide a Writable object to store the real value and an extra boolean value to indicate its validity (Null or Not Null)
 * GET: indicate the node value is retrieved from UDF or ObjectInspector calls,
 *      need to provide a Writable reference to point to that value. 
 * CONSTANT: indicate the node is constant variable, 
 *      need to provide a final Writable reference to that value, which is pre-set during the code generating
 *      ExprNodeConstant
 */
object EvaluationType extends Enumeration {
  val SET, GET, CONSTANT = Value
}

abstract class ExprCodeGen(@BeanProperty val context : CGContext) extends LogHelper {
  def evaluationType() = EvaluationType.GET
  
  private lazy val DEFAULT_VALIDATE_CHECK_CODE : String = {
    evaluationType() match {
      case EvaluationType.GET => resultVariableName() + "!=null"
      case EvaluationType.CONSTANT => if (constantNull()) "false" else null
      case EvaluationType.SET => nullValueIndicatorVariableName()
    }
  }
  
  private var codeValidateCheck : String = _
  private def getCodeValidateCheck() = {
    if (useDefaultValidateCheckCode)
      DEFAULT_VALIDATE_CHECK_CODE
    else
      codeValidateCheck
  }
  
  private var cgValidateCheckCodeGenerated : Boolean = false
  private var useDefaultValidateCheckCode = true

  final def cgValidateCheck() : String = {
    if (!cgValidateCheckCodeGenerated) {
      cgValidateCheckCodeGenerated = true
      return getCodeValidateCheck()
    }

    null
  }

  protected final def setCgValidateCheck(cgValidateCheck : String) {
    cgValidateCheckCodeGenerated = false
    this.codeValidateCheck = cgValidateCheck
    useDefaultValidateCheckCode = false
  }

  def notifyEvaluatingCodeGenNeed() {
    this.cgValidateCheckCodeGenerated = false;
  }

  def nullValueIndicatorVariableName() : String = null
  def initValueExpr() : String = null
  def invalidValueExpr() : String = null
  def constantNull() = false
  def resultVariableType() : Class[_]
  def resultVariableName() : String
  def codeInit() : LinkedHashSet[String]
  def codeEvaluate() : String
  def initialize(rowInspector : ObjectInspector) : ObjectInspector
  
  def valueInJavaType() : String = {
    if (constantNull()) return "null"
    var format = getCodeValidateCheck()
    if (format != null) format += "? %s : null" else format = "%s"
    
    val rvt = resultVariableType()
    
    rvt match {
      case _ if rvt == classOf[org.apache.hadoop.io.BooleanWritable] => format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.hive.serde2.io.ByteWritable] => format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable] => format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.io.FloatWritable] =>format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.io.IntWritable] => format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.io.LongWritable] =>format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.hive.serde2.io.ShortWritable] =>format.format(resultVariableName + ".get()")
      case _ if rvt == classOf[org.apache.hadoop.io.Text] => format.format(resultVariableName + ".toString()")
      case _ if rvt == classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable] => format.format(resultVariableName + ".getTimestamp()")
      case _ if rvt == classOf[org.apache.hadoop.io.NullWritable] => "null"
      case other => throw new CGAssertRuntimeException("Class:[" + other.getCanonicalName() + "] cannot be handled properly.")
    }
  }
}