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

import java.util.ArrayList
import scala.reflect.BeanProperty
import scala.collection.mutable.LinkedHashSet

import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr
import org.apache.hadoop.hive.ql.exec.UDF

import shark.execution.cg.udf._
import shark.execution.cg.node._

object ValueType extends Enumeration {
  type ValueType = Value
  val TYPE_VALUE = Value("value")
  val TYPE_NULL_INDICATOR = Value("nullIndicator")
  val TYPE_UDF = Value("udf")
  val TYPE_UDAF = Value("udaf")
  val TYPE_CONVERTER = Value("converter")
}

class CGContext {
  private val factory = new CodeGenFactory(this)
  import ValueType._

  @BeanProperty val definitions = new LinkedHashSet[Definition]()
  private[this] var baseValue = 0

  def isVariableDefined(variableName : String) = {
    var result = false
    definitions.foreach((d : Definition) => { result ||= (d.defName == variableName) })

    result
  }

  private def createVariableName(prefix : String) = {
    baseValue += 1
    prefix + "_" + baseValue
  }

  /**
   * create value variable name (which will appear in the property of the generated class), the function call order doens't mean the same
   * order in the generated class;
   * @param type
   * @param classType
   * @param createObjectInDefinition
   */
  def createValueVariableName(t : ValueType, classType : Class[_], createObjectInDefinition : Boolean) : String =
    createValueVariableName(t, classType, createObjectInDefinition, false, false, null)

  def createValueVariableName(t : ValueType, classType : Class[_], variableName : String, 
      createObjectInDefinition : Boolean) : String =
    createValueVariableName(t, classType, variableName, createObjectInDefinition, false, false, null)

  def createValueVariableName(t : ValueType, classType : Class[_], createObjectInDefinition : Boolean, 
      isFinal : Boolean, isStatic : Boolean, initString : String) : String =
    createValueVariableName(t, classType, createVariableName(t.toString()), 
        createObjectInDefinition, isFinal, isStatic, initString)

  def createValueVariableName(t : ValueType, classType : Class[_], variableName : String, 
      createObjectInDefinition : Boolean, isFinal : Boolean, isStatic : Boolean, initString : String) : String = {
    t match {
      case ValueType.TYPE_NULL_INDICATOR => definitions.add(
          new Definition(classOf[Boolean], variableName, true, false, false, " = false"))
      case _                             => definitions.add(
          new Definition(classType, variableName, createObjectInDefinition, isFinal, isStatic, initString))
    }

    variableName
  }

  def createValueVariableName(t : ValueType, classType : Class[_]) : String =
    createValueVariableName(t, classType, true)
  def createValueVariableName(t : ValueType, classType : Class[_], value : String) : String =
    createValueVariableName(t, classType, value, true)
  def createNullIndicatorVariableName() : String =
    createValueVariableName(ValueType.TYPE_NULL_INDICATOR, classOf[Boolean], false)

  def create(node : GenericFunExprNodeCodeGen) = factory.create(node)

  def create(udf : GenericUDFBridge, node : GenericFunExprNodeCodeGen) = factory.create(udf, node)

  def create(outputOI : ConstantObjectInspector) = factory.create(outputOI)

  def create(desc : ExprNodeDesc) = factory.create(desc)
}