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
  
  val TYPE_NULL_INDICATOR = Value("nullIndicator")
  val TYPE_VALUE = Value("value")
  val TYPE_UDF = Value("udf")
  val TYPE_UDAF = Value("udaf")
  val TYPE_CONVERTER = Value("converter")
}

class CGContext {
  import ValueType._

  @BeanProperty val fieldDefinitions = new LinkedHashSet[PropertyDefinition]()
  private var baseValue = 0
  private val factory = new CGNodeFactory(this)
  private val imports = new LinkedHashSet[String]()
  
  /**
   * Register the imports
   */
  def registerImport(clazz: Class[_]) {
    if (!clazz.getCanonicalName().startsWith("java.lang")) {
      imports.add(clazz.getCanonicalName() + ";")
    }
  }
  
  def getImports() = imports
  
  /**
   * Check if the variable defined (existed in the field definitions set)
   */
  def isVariableDefined(variableName: String) = {
    var result = false
    fieldDefinitions.foreach((d: PropertyDefinition) => { result ||= (d.defName == variableName) })

    result
  }

  /**
   * create a unique[within the generated java class] variable name, with specified prefix
   */
  private def createVariableName(prefix: String) = {
    baseValue += 1
    prefix + "_" + baseValue
  }

  /**
   * The helper function is to create the Field Definition object and add it into the field set, 
   * which contains all field variables of the generated java class
   * 
   * @param t currently we only cares TYPE_NULL_INDICATOR and non-TYPE_NULL_INDICATOR
   *          TYPE_NULL_INDICATOR indicates the field class type is Boolean. 
   * @param classType field variable type (in Java Class)
   * @param variableName the field variable name
   * @param createInstance indicate if required to create the field instance while in declaration
   * @param isFinal indicate if the field should be with the modifier "final"
   * @param isStatic indicate if the field should be with the modifier "static" 
   * @param initString specify the customized initializing code snippet for the field variable in
   *        declaration 
   * @return the variable name of the created Field name 
   */
  def createValueVariableName(t: ValueType,
                              classType: Class[_],
                              variableName: String,
                              createInstance: Boolean,
                              isFinal: Boolean,
                              isStatic: Boolean,
                              initString: String) = {
    t match {
      case ValueType.TYPE_NULL_INDICATOR => fieldDefinitions.add(
        new PropertyDefinition(classOf[Boolean], variableName, true, false, false, " = false"))
      case _ => fieldDefinitions.add(
        new PropertyDefinition(classType, variableName, createInstance, isFinal, isStatic, initString))
    }

    variableName
  }
  
  def createValueVariableName(t: ValueType,
                              classType: Class[_],
                              createInstance: Boolean): String =
    createValueVariableName(t,
      classType,
      createInstance,
      false,
      false,
      null
    )

  def createValueVariableName(t: ValueType,
                              classType: Class[_],
                              variableName: String,
                              createInstance: Boolean): String =
    createValueVariableName(
      t,
      classType,
      variableName,
      createInstance,
      false,
      false,
      null)

  def createValueVariableName(t: ValueType,
                              classType: Class[_],
                              createInstance: Boolean,
                              isFinal: Boolean,
                              isStatic: Boolean,
                              initString: String): String =
    createValueVariableName(t,
      classType,
      createVariableName(t.toString()),
      createInstance,
      isFinal,
      isStatic,
      initString)

  def createValueVariableName(t: ValueType, classType: Class[_]): String =
    createValueVariableName(
      t,
      classType,
      true)

  def createValueVariableName(t: ValueType, classType: Class[_], value: String): String =
    createValueVariableName(
      t,
      classType,
      value,
      true)

  def createNullIndicatorVariableName(): String =
    createValueVariableName(
      ValueType.TYPE_NULL_INDICATOR,
      classOf[Boolean],
      false
    )

  // create the genericUDF based code gen node (as value of the GenericFunExpr)
  def create(node: GenericFunNode) = factory.create(node)

  // create the UDFBridge based code gen node (as value of the GenericFunExpr)
  def create(udf: GenericUDFBridge, node: GenericFunNode) = factory.create(udf, node)

  // create a code gen node via the ExprNodeDesc
  def create(desc: ExprNodeDesc) = factory.create(desc)
}