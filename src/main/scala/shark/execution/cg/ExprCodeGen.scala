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

import scala.collection.mutable.LinkedHashSet
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector

import shark.LogHelper



/**
 * VarType represent the variable type in the generated code
 * Usually there are 3 types
 * SET: indicate the node value is retrieved from the re-implemented UDF/GenericUDF,
 *      need to provide a Writable object to store the real value and an extra boolean value 
 *      to indicate its validity (Null or Not Null)
 * NOT_NULL: as the the SET, but the the node value can NOT be null constantly
 * GET: indicate the node value is retrieved from UDF or ObjectInspector calls,
 *      need to provide a Writable reference to point to that value.
 * CONSTANT: indicate the node is constant variable,
 *      need to provide a final Writable reference to that value, which is pre-set during 
 *      the code generating ExprNodeConstant
 * NULL: always be null value
 */
object EvaluationType extends Enumeration {
  val SET, NOT_NULL, GET, CONSTANT, NULL = Value
}

/**
 * the listener, which code branch changed in codegen, 
 */
trait CodeBranchChangeListener {
  def notifycgValidateCheckCodeNeed() { }
  def notifyEvaluatingCodeGenNeed() { }
}
  
abstract class ExprCodeGen(@BeanProperty val context: CGContext) 
  extends LogHelper 
  with CodeBranchChangeListener {
  
  def evaluationType() = EvaluationType.GET

  @BeanProperty var outputInspector: ObjectInspector = null
  
  var codeValidationSnippet:()=>String = ()=>{
    evaluationType() match {
      case EvaluationType.GET      => resultVariableName() + "!=null"
      case EvaluationType.NOT_NULL => null
      case EvaluationType.CONSTANT => "true"
      case EvaluationType.NULL     => "false"
      case EvaluationType.SET      => nullValueIndicatorVariableName()
    }
  }
  
  private var cgValidateCheckCodeGenerated: Boolean = false

  /** 
   * Get the writable class from the ObjectInspector
   */
  protected def getWritableClass(poi:ObjectInspector) =
    PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
         poi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory()).primitiveWritableClass

  
  /**
   * Normally, we need to check if the current node is valid before passed to its parent node in
   * gen code. The switch "cgValidateCheckCodeGenerated" guarantees the checking code only be 
   * produced once in the executing branch, in the gen code.
   */
  final def cgValidateCheck(): String = {
    if (!cgValidateCheckCodeGenerated) {
      cgValidateCheckCodeGenerated = true
      return codeValidationSnippet()
    }

    null
  }

  /**
   * Need to generate the value check code
   */
  override def notifycgValidateCheckCodeNeed() {
    this.cgValidateCheckCodeGenerated = false
  }
  
  /**
   * indicator variable name (in the gen code), which to represent if the current node 
   * is null(or invalid), its type in the gen code is boolean (true for valid otherwise false)
   */
  def nullValueIndicatorVariableName(): String = null
  
  /**
   * code snippet of initialize the value of current node in the gen code
   */
  def initValueExpr(): String = {
    evaluationType() match {
      case EvaluationType.GET      => resultVariableName() + "=null"
      case EvaluationType.NOT_NULL => null
      case EvaluationType.CONSTANT => null
      case EvaluationType.NULL     => null
      case EvaluationType.SET      => nullValueIndicatorVariableName() + "=true"
    }
  }

  /**
   * code snippet of mark the value of current node as invalid
   */
  def invalidValueExpr(): String = {
    evaluationType() match {
      case EvaluationType.GET      => resultVariableName() + "=null"
      case EvaluationType.NOT_NULL => null
      case EvaluationType.CONSTANT => null
      case EvaluationType.NULL     => null
      case EvaluationType.SET      => nullValueIndicatorVariableName() + "=false"
    }
  }
  
  /**
   * variable type represents the current expr value
   */
  def resultVariableType(): Class[_] = getWritableClass(getOutputInspector())

  /**
   * Code snippet in the "init(ObjectInspector oi)" method of the gen code
   */
  def codeInit(): LinkedHashSet[String]
  
  /**
   * current expression value of the generated code
   */
  def valueExpr() = codeValueExpr()
  var codeValueExpr:()=>String = ()=>{resultVariableName()}
  
  /**
   * variable name represents the current expr value, in the gen code
   */
  def resultVariableName() = codeWritableNameSnippet()
  var codeWritableNameSnippet:() => String = ()=>{null}
  
  /**
   * Code snippet in the "evaluate(Object row)" method of the gen code
   */
  def cgEvaluate(): String = codeEvaluateSnippet()
  var codeEvaluateSnippet:()=>String = ()=>{null}
  
  /**
   * The implementation has the responsibility to call "fold" method for its children nodes.
   * @param rowInspector the row ObjectInspector
   * @return Boolean false for can not handle the rowInspector(if it's Array/Map based 
   * ObjectInspector or UDF), otherwise true  
   */
  def fold(rowInspector: ObjectInspector): Boolean
  def isStateful() = false
  def isDeterministic() = true
  def constantNull() = false
  
  /**
   * TODO, primitive object only.
   * down-cast to primitive object inspector
   */
  def castToPrimitiveObjectInspector(oi: ObjectInspector): PrimitiveObjectInspector = {
    if (oi == null) {
      null
    } else if (!oi.isInstanceOf[PrimitiveObjectInspector]) {
      logWarning("cannot handle " + oi.getClass().getCanonicalName())
      null
    } else {
      oi.asInstanceOf[PrimitiveObjectInspector]
    }
  }
  
  def typeInfo() = TypeInfoUtils.getTypeInfoFromObjectInspector(this.outputInspector)

  // may cause problem in debugging, which means to call the valueExpr() before "fold" function 
  // called, anyway, toString() would be great helpful for retrieving the expression value 
  // code snippet in its subclasses 
  override def toString() = valueExpr()
}
