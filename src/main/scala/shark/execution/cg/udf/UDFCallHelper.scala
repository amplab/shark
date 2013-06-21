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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import shark.execution.cg.CGAssertRuntimeException


/**
 * "protected def cgUDFCall(): ()=>String" function of UDFCodeGen, probably need to dispatch to 
 *  concrete type-based handling.
 */
trait UDFCallHelper {
  def resultVariableName(): String
  
  def cgUDFCall(typeinfo: TypeInfo) = {
    typeinfo match {
      case TypeInfoFactory.voidTypeInfo => cgUDFCallVoid()
      case TypeInfoFactory.unknownTypeInfo =>
        // TODO return cgUDFCall_UNKNOWN(v1, v2, udfVariableName);
        throw new CGAssertRuntimeException("UNKNOWN(Map/List/Union etc.) " +
          "type binary function calls haven't been implemented yet.")
      case TypeInfoFactory.booleanTypeInfo   => cgUDFCallBoolean()
      case TypeInfoFactory.byteTypeInfo      => cgUDFCallByte()
      case TypeInfoFactory.stringTypeInfo    => cgUDFCallString()
      case TypeInfoFactory.shortTypeInfo     => cgUDFCallShort()
      case TypeInfoFactory.intTypeInfo       => cgUDFCallInt()
      case TypeInfoFactory.longTypeInfo      => cgUDFCallLong()
      case TypeInfoFactory.floatTypeInfo     => cgUDFCallFloat()
      case TypeInfoFactory.doubleTypeInfo    => cgUDFCallDouble()
      case TypeInfoFactory.timestampTypeInfo => cgUDFCallTimestamp()
      case TypeInfoFactory.dateTypeInfo      => cgUDFCallDate()
      case TypeInfoFactory.binaryTypeInfo    => cgUDFCallBinary()
      case null => null
      case _ => throw new CGAssertRuntimeException("More type should be added.")
    }
  }

  // the following function is call indirectly, not necessary to be the ()=>String
  protected def cgUDFCallText(): String = null
  protected def cgUDFCallPrimitive(): String = null
  protected def cgUDFCallComparable(): String = null
  
  // default return null code snippet, which mean the the UDF doens't support this type
  protected def cgUDFCallBoolean(): () => String = null
  protected def cgUDFCallVoid(): () => String = null
  protected def cgUDFCallUnknown(): () => String = null

  protected def cgUDFCallByte() = helper(cgUDFCallPrimitive())
  protected def cgUDFCallShort() = helper(cgUDFCallPrimitive())
  protected def cgUDFCallInt() = helper(cgUDFCallPrimitive())
  protected def cgUDFCallLong() = helper(cgUDFCallPrimitive())
  protected def cgUDFCallDouble() = helper(cgUDFCallPrimitive())
  protected def cgUDFCallFloat() = helper(cgUDFCallPrimitive())
  protected def cgUDFCallTimestamp() = helper(cgUDFCallComparable())
  protected def cgUDFCallDate() = helper(cgUDFCallComparable())
  protected def cgUDFCallBinary() = helper(cgUDFCallComparable())
  protected def cgUDFCallString() = helper(cgUDFCallText())
  
  private def helper(code: String) = {
    if (code != null)
      () => resultVariableName() + ".set(" + code + ");"
    else
      null
  }
}