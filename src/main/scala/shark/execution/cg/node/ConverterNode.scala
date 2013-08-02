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

import java.sql.Timestamp
import java.util.Arrays

import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text

import scala.reflect.BeanProperty

import shark.LogHelper
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.ExprCodeGen
import shark.execution.cg.EvaluationType
import shark.execution.cg.ValueType
import shark.execution.cg.CGContext


/**
 * Converter pattern types
 */
object ConverterType extends Enumeration {
  type ConverterType = Value
  val DIRECT_GET, HIVE_CONVERTER, CHECKED_CAST, DIRECT_CAST, 
      HIVE_CONVERTER_DIRECT_CAST, LIMITED_TYPE = Value
}

/**
 * Please refer to 
 * org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
 */
sealed trait PrimitiveValueWithCast {
  protected def setConstantNull(): String
  
  /**
   * utility to get the primitive value, default is "writable.getXXX()", 
   * but the TimestampWritable and DateWritable still return its writable object variable name, 
   * as they don't have the primitive value. 
   */
  protected def cgPrimitiveValue(writableClass: Class[_], variableName: String) = {
    if (writableClass == classOf[org.apache.hadoop.io.BooleanWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.ByteWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.io.FloatWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.io.IntWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.io.LongWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.ShortWritable])
      variableName + ".get()"
    else if (writableClass == classOf[org.apache.hadoop.io.Text])
      variableName + ".toString()"
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable])
      variableName
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.DateWritable])
      variableName
    else if (writableClass == classOf[org.apache.hadoop.io.NullWritable])
      "null"
    else
      throw new CGAssertRuntimeException("Cannot handle class " + writableClass.getCanonicalName())
  }
  
  protected def valueJWithCast(context: CGContext, 
                               from: PrimitiveObjectInspector, 
                               to: PrimitiveCategory, 
                               primitiveValueExpr: String) = {
    to match {
      case PrimitiveCategory.VOID =>
        throw new CGAssertRuntimeException("doesn't support the converting to void")
      case PrimitiveCategory.BOOLEAN   => getBooleanValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.BYTE      => getByteValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.SHORT     => getShortValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.INT       => getIntValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.LONG      => getLongValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.FLOAT     => getFloatValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.DOUBLE    => getDoubleValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.STRING    => getStringValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.DATE      => getDateValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.TIMESTAMP => getTimestampValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.BINARY    => getBinaryValue(context, from, primitiveValueExpr)
      case PrimitiveCategory.UNKNOWN =>
        throw new CGAssertRuntimeException("doesn't support the converting to unknown")
      case _ =>
        throw new CGAssertRuntimeException("More type should be added.")
    }
  }
    
  // the following code is all about the data type conversion from java type to another java type
  protected def getBooleanValue(context: CGContext, 
                                from: PrimitiveObjectInspector, 
                                primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "false"
      case PrimitiveCategory.BOOLEAN   => primitiveValueExpr
      case PrimitiveCategory.BYTE      => "(" + primitiveValueExpr + "==0 ? false: true)"
      case PrimitiveCategory.SHORT     => "(" + primitiveValueExpr + "==0 ? false: true)"
      case PrimitiveCategory.INT       => "(" + primitiveValueExpr + "==0 ? false: true)"
      case PrimitiveCategory.LONG      => "(" + primitiveValueExpr + "==0 ? false: true)"
      case PrimitiveCategory.FLOAT     => "(" + primitiveValueExpr + "==0 ? false: true)"
      case PrimitiveCategory.DOUBLE    => "(" + primitiveValueExpr + "==0 ? false: true)"
      case PrimitiveCategory.STRING    => "(" + primitiveValueExpr + ".length() >0 ? true : false)"
      case PrimitiveCategory.DATE      => "(" + primitiveValueExpr + ".getTimeInSeconds() == 0 ? false : true)"
      case PrimitiveCategory.TIMESTAMP => "(" + primitiveValueExpr + ".getSeconds() == 0 ? false : true)"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getByteValue(context: CGContext, 
                             from: PrimitiveObjectInspector, 
                             primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "(byte)0"
      case PrimitiveCategory.BOOLEAN   => "(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => primitiveValueExpr
      case PrimitiveCategory.SHORT     => "(byte)" + primitiveValueExpr
      case PrimitiveCategory.INT       => "(byte)" + primitiveValueExpr
      case PrimitiveCategory.LONG      => "(byte)" + primitiveValueExpr
      case PrimitiveCategory.FLOAT     => "(byte)" + primitiveValueExpr
      case PrimitiveCategory.DOUBLE    => "(byte)" + primitiveValueExpr
      case PrimitiveCategory.STRING    => "(byte)(Integer.parseInt(" + primitiveValueExpr + "))"
      case PrimitiveCategory.DATE      => "(byte)(" + primitiveValueExpr + ".getTimeInSeconds())"
      case PrimitiveCategory.TIMESTAMP => "(byte)(" + primitiveValueExpr + ".getSeconds())"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getShortValue(context: CGContext, 
                              from: PrimitiveObjectInspector, 
                              primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "(short)0"
      case PrimitiveCategory.BOOLEAN   => "(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => primitiveValueExpr
      case PrimitiveCategory.SHORT     => primitiveValueExpr
      case PrimitiveCategory.INT       => "(short)" + primitiveValueExpr
      case PrimitiveCategory.LONG      => "(short)" + primitiveValueExpr
      case PrimitiveCategory.FLOAT     => "(short)" + primitiveValueExpr
      case PrimitiveCategory.DOUBLE    => "(short)" + primitiveValueExpr
      case PrimitiveCategory.STRING    => "(short)(Integer.parseInt(" + primitiveValueExpr + "))"
      case PrimitiveCategory.DATE      => "(short)(" + primitiveValueExpr + ".getTimeInSeconds())"
      case PrimitiveCategory.TIMESTAMP => "(short)(" + primitiveValueExpr + ".getSeconds())"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getIntValue(context: CGContext, 
                            from: PrimitiveObjectInspector, 
                            primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "0"
      case PrimitiveCategory.BOOLEAN   => "(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => primitiveValueExpr
      case PrimitiveCategory.SHORT     => primitiveValueExpr
      case PrimitiveCategory.INT       => primitiveValueExpr
      case PrimitiveCategory.LONG      => "(int)" + primitiveValueExpr
      case PrimitiveCategory.FLOAT     => "(int)" + primitiveValueExpr
      case PrimitiveCategory.DOUBLE    => "(int)" + primitiveValueExpr
      case PrimitiveCategory.STRING    => "(int)(Integer.parseInt(" + primitiveValueExpr + "))"
      case PrimitiveCategory.DATE      => "(int)(" + primitiveValueExpr + ".getTimeInSeconds())"
      case PrimitiveCategory.TIMESTAMP => "(int)(" + primitiveValueExpr + ".getSeconds())"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getFloatValue(context: CGContext, 
                              from: PrimitiveObjectInspector, 
                              primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "0f"
      case PrimitiveCategory.BOOLEAN   => "(float)(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => "(float)" + primitiveValueExpr
      case PrimitiveCategory.SHORT     => "(float)" + primitiveValueExpr
      case PrimitiveCategory.INT       => "(float)" + primitiveValueExpr
      case PrimitiveCategory.LONG      => "(float)" + primitiveValueExpr
      case PrimitiveCategory.FLOAT     => primitiveValueExpr
      case PrimitiveCategory.DOUBLE    => "(float)" + primitiveValueExpr
      case PrimitiveCategory.STRING    => "Float.parseFloat(" + primitiveValueExpr + ")"
      case PrimitiveCategory.DATE      => "(float)(" + primitiveValueExpr + ".getTimeInSeconds())"
      case PrimitiveCategory.TIMESTAMP => "(float)(" + primitiveValueExpr + ".getDouble())"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getLongValue(context: CGContext, 
                             from: PrimitiveObjectInspector, 
                             primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "0l"
      case PrimitiveCategory.BOOLEAN   => "(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => primitiveValueExpr
      case PrimitiveCategory.SHORT     => primitiveValueExpr
      case PrimitiveCategory.INT       => primitiveValueExpr
      case PrimitiveCategory.LONG      => primitiveValueExpr
      case PrimitiveCategory.FLOAT     => "(long)" + primitiveValueExpr
      case PrimitiveCategory.DOUBLE    => "(long)" + primitiveValueExpr
      case PrimitiveCategory.STRING    => "Long.parseLong(" + primitiveValueExpr + ")"
      case PrimitiveCategory.DATE      => "(long)(" + primitiveValueExpr + ".getTimeInSeconds())"
      case PrimitiveCategory.TIMESTAMP => "(long)(" + primitiveValueExpr + ".getSeconds())"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getDoubleValue(context: CGContext, 
                               from: PrimitiveObjectInspector, 
                               primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => "0.0d"
      case PrimitiveCategory.BOOLEAN   => "(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => primitiveValueExpr
      case PrimitiveCategory.SHORT     => primitiveValueExpr
      case PrimitiveCategory.INT       => primitiveValueExpr
      case PrimitiveCategory.LONG      => primitiveValueExpr
      case PrimitiveCategory.FLOAT     => primitiveValueExpr
      case PrimitiveCategory.DOUBLE    => primitiveValueExpr
      case PrimitiveCategory.STRING    => "Double.parseDouble(" + primitiveValueExpr + ")"
      case PrimitiveCategory.DATE      => primitiveValueExpr + ".getTimeInSeconds()"
      case PrimitiveCategory.TIMESTAMP => primitiveValueExpr + ".getDouble()"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getStringValue(context: CGContext, 
                               from: PrimitiveObjectInspector, 
                               primitiveValueExpr: String): String = {
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => setConstantNull()
      case PrimitiveCategory.BOOLEAN   => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.BYTE      => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.SHORT     => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.INT       => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.LONG      => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.FLOAT     => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.DOUBLE    => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.STRING    => primitiveValueExpr
      case PrimitiveCategory.DATE      => primitiveValueExpr + ".toString()"
      case PrimitiveCategory.TIMESTAMP => primitiveValueExpr + ".toString()"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getDateValue(context: CGContext, 
                             from: PrimitiveObjectInspector, 
                             primitiveValueExpr: String): String = {
    context.registerImport(classOf[org.apache.hadoop.hive.serde2.io.DateWritable])
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => setConstantNull()
      case PrimitiveCategory.BOOLEAN   => 
        "DateWritable.timeToDate(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => "DateWritable.timeToDate(" + primitiveValueExpr + ")"
      case PrimitiveCategory.SHORT     => "DateWritable.timeToDate(" + primitiveValueExpr + ")"
      case PrimitiveCategory.INT       => "DateWritable.timeToDate(" + primitiveValueExpr + ")"
      case PrimitiveCategory.LONG      => "DateWritable.timeToDate(" + primitiveValueExpr + ")"
      case PrimitiveCategory.FLOAT     => 
        "DateWritable.timeToDate((long)" + primitiveValueExpr + ")"
      case PrimitiveCategory.DOUBLE    => 
        "DateWritable.timeToDate((long)" + primitiveValueExpr + ")"
      case PrimitiveCategory.STRING    => "Date.valueOf(" + primitiveValueExpr + ")"
      case PrimitiveCategory.DATE      => primitiveValueExpr + ".get()"
      case PrimitiveCategory.TIMESTAMP => 
        "DateWritable.timeToDate(" + primitiveValueExpr + ".getSeconds())"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }

  protected def getTimestampValue(context: CGContext, 
                                  from: PrimitiveObjectInspector, 
                                  primitiveValueExpr: String): String = {
    context.registerImport(classOf[java.sql.Timestamp])
    context.registerImport(classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable])
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => setConstantNull()
      case PrimitiveCategory.BOOLEAN   => "new Timestamp(" + primitiveValueExpr + "? 1 : 0)"
      case PrimitiveCategory.BYTE      => "new Timestamp(" + primitiveValueExpr + ")"
      case PrimitiveCategory.SHORT     => "new Timestamp(" + primitiveValueExpr + ")"
      case PrimitiveCategory.INT       => "new Timestamp(" + primitiveValueExpr + ")"
      case PrimitiveCategory.LONG      => "new Timestamp(" + primitiveValueExpr + ")"
      case PrimitiveCategory.FLOAT     => 
        "TimestampWritable.floatToTimestamp(" + primitiveValueExpr + ")"
      case PrimitiveCategory.DOUBLE    => 
        "TimestampWritable.doubleToTimestamp(" + primitiveValueExpr + ")"
      case PrimitiveCategory.STRING    => "Timestamp.valueOf(" + primitiveValueExpr + ")"
      case PrimitiveCategory.DATE      => "new Timestamp(" + primitiveValueExpr + ".getTime())"
      case PrimitiveCategory.TIMESTAMP => primitiveValueExpr + ".getTimestamp()"
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory());
    }
  }

  protected def getBinaryValue(context: CGContext, 
                               from: PrimitiveObjectInspector, 
                               primitiveValueExpr: String): String = {
    context.registerImport(classOf[java.util.Arrays])
    from.getPrimitiveCategory() match {
      case PrimitiveCategory.VOID      => setConstantNull()
      case PrimitiveCategory.STRING    => "%s.getBytes()".format(primitiveValueExpr)
      case PrimitiveCategory.BINARY    => primitiveValueExpr
      case _ => throw new RuntimeException("Hive 2 Internal error: unknown type: " + 
          from.getPrimitiveCategory())
    }
  }
}

/**
 * Root CodeGen class of data type converting.
 * e.g. evaluating code as:
 * IntWritable a = xxx
 * 
 * The toString() method returns the string "a", which means it's not a real data type converting.
 */
sealed class ConverterNode[T<:ExprNodeDesc](
    node: ExprNode[T], 
    expectedOI:ObjectInspector)
  extends ExprNode[T](node.context, null.asInstanceOf[T]) with LogHelper {

  protected var delegate:ExprNode[ExprNodeDesc] = node
  protected var codeJavaVariableNameSnippet:() => String = ()=>{null}
  protected var from: PrimitiveObjectInspector = _
  protected var to: PrimitiveObjectInspector = _

  protected def setConstantNull(): String = {
    delegate = ConverterNode(delegate) // create NullNode
    "null"
  }
  
  protected def formatter(withCheck: Boolean): String = {
    var format = if (withCheck) ConverterNode.this codeValidationSnippet() else null
    if (format != null) format += "? %s : null" else format = "%s"
      
    format
  }
  
  /**
   * utility to get the primitive with specified format
   * default is "writable.getXXX()", will return the primitive value or the Java Object accordingly
   */
  protected def cgJavaValueInFormat(
      writableClass: Class[_], 
      variableName: String, 
      format: String) = {
    if (writableClass == classOf[org.apache.hadoop.io.BooleanWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.ByteWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.io.FloatWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.io.IntWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.io.LongWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.ShortWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.io.Text])
      format.format(variableName + ".toString()")
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable])
      format.format(variableName + ".getTimestamp()")
    else if (writableClass == classOf[org.apache.hadoop.hive.serde2.io.DateWritable])
      format.format(variableName + ".get()")
    else if (writableClass == classOf[org.apache.hadoop.io.NullWritable])
      "null"
    else
      throw new CGAssertRuntimeException("Cannot handle class " + writableClass.getCanonicalName())
  }
  
  /**
   * Get the value expression of primitive type
   */
  protected def primitiveValueStr() = 
    cgJavaValueInFormat(delegate.resultVariableType(), 
                        delegate.resultVariableName(), 
                        formatter(false))

  /**
   * preparing the value expr code string
   * @return false means can not handle the evaluation properly, otherwise true
   */
  protected def initial(constant: Boolean) = {
    codeValidationSnippet = getValidationSnippet(constant)
    codeEvaluateSnippet = getEvaluateSnippet(constant)
    codeWritableNameSnippet = getWritableVariable(constant)
    codeValueExpr = getValueExpr(constant)

    true
  }
  
  // default is to get its primitive value
  protected def getValueExpr(constant: Boolean) = getWritableVariable(constant)
  protected def getEvaluateSnippet(constant: Boolean) =
    ()=>delegate.cgEvaluate()
  protected def getValidationSnippet(constant: Boolean) = 
    ()=>delegate.cgValidateCheck()
  protected def getWritableVariable(constant: Boolean) = 
    ()=>delegate.resultVariableName()
  protected def getJavaVariable(constant: Boolean) = 
    if(constantNull()) {
      ()=>null // throw new CGAssertRuntimeException("can not initiate null for primitive type.")
    } else if (constant) {
      var category = getOutputInspector().
                         asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory()
      var typeEntry = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(category)
      var javaType = typeEntry.primitiveJavaType
      // because the String, Date, Timestamp doesn't have the primitive java type, but class
      if (null == javaType) javaType = typeEntry.primitiveJavaClass

      // create an converted constant variable in java type
      var javaVariableName = getContext().createValueVariableName(
          ValueType.TYPE_VALUE,
          javaType,
          true, // create instance?
          true, // is final?
          false, // is static
          "=" + primitiveValueStr())
      () => javaVariableName
    } else {
      () => primitiveValueStr()
    }

  /**
   * fold the code gen node, via the data type computing. 
   * it means the code gen can not handle the data type properly if false retrieved
   */
  final override def prepare(rowInspector: ObjectInspector): Boolean = {
    // assumes that the delegate has been folded already
    from = castToPrimitiveObjectInspector(delegate.getOutputInspector())
    to = castToPrimitiveObjectInspector(expectedOI)
    if (null == from || null == from) {
      false // couldn't handle the object inspector
    } else {
      from = delegate.getOutputInspector().asInstanceOf[PrimitiveObjectInspector]
      setOutputInspector(to)
      
      initial(from.isInstanceOf[ConstantObjectInspector])
    }
  }

  override def isStateful() = delegate.isStateful()
  override def isDeterministic() = delegate.isDeterministic()
  override def constantNull() = delegate.constantNull()
  override def notifyEvaluatingCodeGenNeed() {
    delegate.notifyEvaluatingCodeGenNeed() 
    super.notifyEvaluatingCodeGenNeed()
  }
  override def notifycgValidateCheckCodeNeed() {
    delegate.notifycgValidateCheckCodeNeed()
    super.notifycgValidateCheckCodeNeed()
  }
  override def evaluationType() = delegate.evaluationType()
  override def invalidValueExpr() = delegate.initValueExpr()
  override def initValueExpr() = delegate.initValueExpr()
  override def codeInit() = delegate.codeInit()
}

/**
 * Codegen for type converting, from Writable to another Writable type, by using Hive Converter.
 * 
 * e.g. evaluating code as:
 * IntWritable a = xxx
 * Converter converter = ObjectInspectorConverters.getConverter(PrimitiveCategory.INT, 
 *                                                              PrimitiveCategory.FLOAT);
 * FloatWritable b = converter.convert(a)
 * 
 * The toString() method returns string of "b"
 * but there are also exceptions:
 * 1) return string of "a" if fromPrimitiveCategory equals toPrimitiveCategory
 * 2) ConstantNull node if "a" is a constant variable, and the converter.convert(a) equals null
 */
sealed class HiveConverterValueNodeCodeGen (
    node: ExprNode[ExprNodeDesc], 
    expectedOI:ObjectInspector)
  extends ConverterNode(node, expectedOI) {
  
  protected var converterName: String = _
  private lazy val constantWritableVariableName = getContext().createValueVariableName(
        ValueType.TYPE_VALUE,
        getWritableClass(to),
        true, // create instance?
        true, // is final?
        false, // is static
        "=(%s)(%s.convert(%s))".format(
            getWritableClass(to).getCanonicalName(), 
            converterName, 
            delegate.resultVariableName()))
        
  private lazy val writableVariableName = getContext().createValueVariableName(
        ValueType.TYPE_VALUE,
        getWritableClass(to),
        false, // create instance?
        false, // is final?
        false, // is static
        null)

  // the following code is about the data type conversion via existed Hive Converters
  private def createConverter(input: PrimitiveObjectInspector,
                      output: PrimitiveObjectInspector) = {
    // if not the same category
    var context = delegate.getContext()

    context.registerImport(classOf[ObjectInspectorConverters])
    context.registerImport(classOf[PrimitiveObjectInspectorFactory])
    context.registerImport(classOf[PrimitiveCategory])
    
    var converterType = classOf[ObjectInspectorConverters.Converter]
    var initStr =
      "= ObjectInspectorConverters.getConverter(" +
        "PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(" +
        "PrimitiveCategory." + input.getPrimitiveCategory().name() + ")," +
        "PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(" +
        "PrimitiveCategory." + output.getPrimitiveCategory().name() + "))"

    // create converter object with initialization
    context.createValueVariableName(
      ValueType.TYPE_CONVERTER,
      converterType,
      true,  // create instance?
      true,  // is final?
      false, // is static
      initStr)
  }
  
  /**
   * Test if the ConstantObjectInspector could be converted
   */
  private def testConvertable(input: PrimitiveObjectInspector,
                      output: PrimitiveObjectInspector, coi: ConstantObjectInspector): Boolean = {
    null != ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            input.getPrimitiveCategory()),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            output.getPrimitiveCategory())
        ).convert(coi.getWritableConstantValue())
  }

  protected override def getEvaluateSnippet(constant: Boolean) =
    if (converterName==null) // if not need converter
       super.getEvaluateSnippet(constant)
    else if (constant)
      () => null.asInstanceOf[String]
    else
      () => {
        var code = delegate.cgEvaluate()
        (if (code == null) "" else code + "\n") + "%s = (%s)(%s.convert(%s));".format(
            resultVariableName(), getWritableClass(to).getCanonicalName(), 
            converterName, delegate.resultVariableName())
      }
  protected override def getValidationSnippet(constant: Boolean) = 
    if (converterName==null) // if not need converter
       super.getValidationSnippet(constant)
    else if (constant)
      () => null.asInstanceOf[String]
    else
      () => resultVariableName() + "!=null"
        
  protected override def getWritableVariable(constant: Boolean) = 
    if (converterName==null) // if not need converter
       super.getWritableVariable(constant)
    else if (constant) {
      () => constantWritableVariableName
    }
    else
      () => writableVariableName

  protected override def primitiveValueStr() = 
    cgJavaValueInFormat(resultVariableType(), resultVariableName(), formatter(false))
    
  protected override def initial(constant: Boolean) = {
    if (from.getPrimitiveCategory() != to.getPrimitiveCategory()) {
      if (constant) {
        if (testConvertable(from, to, 
                            delegate.getOutputInspector().asInstanceOf[ConstantObjectInspector])) {
          converterName = createConverter(from, to)
        } else {
          setConstantNull()
        }
      } else {
        converterName = createConverter(from, to)
      }
    }
    
    super.initial(constant)
  }
}

/**
 * Code Gen for getting/converting primitive java type from the Writable object
 * e.g. evaluating code as:
 * IntWritable a = xxx
 * 
 * The toString() method returns string pattern like  "(double)a.get()" probably if "to" 
 * PrimitiveCategory is PrimitiveCategory.DOUBLE
 * There are exceptions:
 * 1) "a.get()" if "to" PrimitiveCategory is the same with "from" PrimitiveCategory
 * 2) returns "a.get()" (java.sql.Date) if both "from" & "to" are PrimitiveCategory.DATE 
 * 3) returns "a.getTimestamp()" (java.sql.Timestamp) 
 *    if both "from" & "to" are PrimitiveCategory.TIMESTAMP
 * 4) returns "a.toString()" (java.lang.String) if both "from" & "to" are PrimitiveCategory.TEXT
 * 5) RuntimeException throw if can not support the data type converting (e.g. from BINARY to INT)
 * 6) More complicated cases are from(to) PrimitiveCategory.TIMESTAMP/DATE/STRING to (from) the
 *    other primitive categories, please check trait PrimitiveValueWithCast for details
 */
sealed class CastValueNodeCodeGen(node: ExprNode[ExprNodeDesc], oi:ObjectInspector)
  extends ConverterNode(node, oi) with PrimitiveValueWithCast {
  
  protected override def getValueExpr(constant: Boolean) = getJavaVariable(constant)
  protected override def primitiveValueStr() = { 
    valueJWithCast(delegate.getContext(), from, to.getPrimitiveCategory(), 
        cgPrimitiveValue(delegate.resultVariableType(), 
                         delegate.resultVariableName()))
  }
}

/**
 * Code gen for getting primitive java from the Writable object, but will check it's validity 
 * e.g. evaluating code as:
 * IntWritable a = xxx
 * 
 * The toString() method returns string pattern like  "a!=null ? a.get() : null"
 * But for those non primitive types: 
 * 1) returns "a != null ? a.get() : null" (java.sql.Date) 
 * 2) returns "a != null ? a.getTimestamp() : null" (java.sql.Timestamp) 
 * 3) returns "a != null ? a.toString() : null" (java.lang.String)
 */
sealed class CheckedConverterNode(node: ExprNode[ExprNodeDesc], oi:ObjectInspector)
  extends ConverterNode(node, oi) {
  protected override def getValueExpr(constant: Boolean) = getJavaVariable(constant)
  protected override def primitiveValueStr() = {
    cgJavaValueInFormat(delegate.resultVariableType(), 
                        delegate.resultVariableName(), 
                        formatter(true))
  }
}

/**
 * Logically combines function of HiveConverterValueNodeCodeGen & CheckedGetValueNodeCodeGen
 * e.g. evaluating code as:
 * Text a = xxx
 * Converter converter = ObjectInspectorConverters.getConverter(PrimitiveCategory.STRING, 
 *                                                              PrimitiveCategory.DOUBLE);
 * DoubleWritable b = converter.convert(a)
 * 
 * And toString() method returns string pattern like "b!=null ? b.get() : null"
 * 
 */
sealed class HiveCastConverterNode(
    node: ExprNode[ExprNodeDesc], 
    oi:ObjectInspector)
  extends HiveConverterValueNodeCodeGen(node, oi) {
  
  protected override def getValueExpr(constant: Boolean) = getJavaVariable(constant)
  protected override def primitiveValueStr() = { 
    cgJavaValueInFormat(resultVariableType(), resultVariableName(), formatter(false))
  }
}

/**
 * The same functionality with CastValueNodeCodeGen, but the node will be ConstantNull if the "from" 
 * PrimitiveCategory is in the specified list, 
 */
sealed class LimitedTypeConverterNode(
    node: ExprNode[ExprNodeDesc], 
    oi: ObjectInspector, 
    categories: Set[PrimitiveCategory])
  extends CastValueNodeCodeGen(node, oi) {

  protected override def initial(constant: Boolean) = {
    if (!categories(from.getPrimitiveCategory())) { 
      setConstantNull()
      true // could handle but it is constant null
    } else 
      super.initial(constant)
  }
}

object ConverterNode {
  import shark.execution.cg.node.ConverterType._
  
  def apply(delegate: ExprNode[_]):ExprNode[ExprNodeDesc] = {
    var typeInfo = new PrimitiveTypeInfo()
    var typeName = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
        delegate.getOutputInspector().asInstanceOf[PrimitiveObjectInspector].
          getPrimitiveCategory()).typeName
    
    typeInfo.setTypeName(typeName)
    var node = delegate.getContext().create(new ExprNodeConstantDesc(typeInfo, null)).
                 asInstanceOf[ExprNode[ExprNodeDesc]]
    if (!node.prepare(null)) {
      throw new CGAssertRuntimeException("Should be true, but false")
    }
    
    node
  }
  
  def apply(delegate: ExprNode[ExprNodeDesc], 
      expectedOI: ObjectInspector, 
      cType: ConverterType=ConverterType.DIRECT_GET, 
      categories: Set[PrimitiveCategory] = Set()): ExprNode[ExprNodeDesc] = {
    if (null == expectedOI) {
      // replace to ConstantNode
      throw new CGAssertRuntimeException("expectedOI can not be null")
    }
    
    cType match {
      case ConverterType.DIRECT_GET => 
        new ConverterNode(delegate, expectedOI)
      case ConverterType.CHECKED_CAST => 
        new CheckedConverterNode(delegate, expectedOI)
      case ConverterType.DIRECT_CAST => 
        new CastValueNodeCodeGen(delegate, expectedOI)
      case ConverterType.HIVE_CONVERTER => 
        new HiveConverterValueNodeCodeGen(delegate, expectedOI)
      case ConverterType.LIMITED_TYPE => 
        new LimitedTypeConverterNode(delegate, delegate.getOutputInspector(), categories)
      case ConverterType.HIVE_CONVERTER_DIRECT_CAST => 
        new HiveCastConverterNode(delegate, expectedOI)
      case _ => throw new CGAssertRuntimeException ("need to add more code here.")
    }
  }
}