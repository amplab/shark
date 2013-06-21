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

import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.node.ExprNode
import shark.execution.cg.node.GenericFunNode
import shark.execution.cg.node.ConverterNode
import shark.execution.cg.node.ConverterType
import shark.execution.cg.EvaluationType
import shark.execution.cg.ExprCodeGen

/**
 * Popular Binary UDF(generic) re-implementations( not complete )
 */
sealed abstract class BinaryUDF(node: GenericFunNode)
  extends UDFCodeGen(node) 
  with UDFCallHelper {
  
  protected var v1: ExprNode[ExprNodeDesc] = _
  protected var v2: ExprNode[ExprNodeDesc] = _
  protected var commonTypeInfo: TypeInfo = _
  protected var oiTypeInfo: Array[TypeInfo] = new Array[TypeInfo](2)

  // As built-in binary udf, requires all of the parameters to be in same type,
  // otherwise, we may need to convert it
  protected def initCompareType() = {
    if (oiTypeInfo(0) != oiTypeInfo(1)) {
      if (oiTypeInfo(0) == TypeInfoFactory.stringTypeInfo ||
        oiTypeInfo(1) == TypeInfoFactory.stringTypeInfo) {
        // conform to Hive behavior, if not the same type, and one of the
        // type is string, then convert to double type info
        TypeInfoFactory.doubleTypeInfo
      } else {
        var compareType = FunctionRegistry.getCommonClass(oiTypeInfo(0), oiTypeInfo(1))
        // if can not find the common type, than default type info is double
        if (compareType == null) TypeInfoFactory.doubleTypeInfo else compareType
      }
    } else {
      oiTypeInfo(0)
    }
  }

  protected def initConvertType(paramIdx: Int) = {
    if (commonTypeInfo == TypeInfoFactory.doubleTypeInfo &&
       (oiTypeInfo(paramIdx)== TypeInfoFactory.stringTypeInfo ||
        oiTypeInfo(paramIdx)== TypeInfoFactory.binaryTypeInfo ||
        oiTypeInfo(paramIdx)== TypeInfoFactory.timestampTypeInfo ||
        oiTypeInfo(paramIdx)== TypeInfoFactory.dateTypeInfo))
      // if the node is the non java primitive object and to be casted into double, we need to
      // convert it to DoubleWritable (via Hive Converter utilities) first, and then cast to double
      ConverterType.HIVE_CONVERTER_DIRECT_CAST
    else if (commonTypeInfo == TypeInfoFactory.stringTypeInfo || // convert to TEXT
      commonTypeInfo == TypeInfoFactory.timestampTypeInfo || // convert to comparable
      commonTypeInfo == TypeInfoFactory.dateTypeInfo) // convert to comparable
      ConverterType.HIVE_CONVERTER
    else
      // should be cast the Writable object into java primitive directly 
      ConverterType.DIRECT_CAST
  }
  
  // will try to reuse the the result object (WritableObject), but need a null indicator
  override def evaluationType() = EvaluationType.SET  

  // set the result object invalid
  override def invalidValueExpr() = nullValueIndicatorVariableName() + "=false"
  
  // reset the result object as valid
  override def initValueExpr() = nullValueIndicatorVariableName() + "=true"

  override def prepare(rowInspector: ObjectInspector, children: Array[_<:ExprNode[ExprNodeDesc]]) = {
    if (children.size != 2) {
      throw new CGAssertRuntimeException("expected 2 arguments in the BinaryUDF")
    }
    oiTypeInfo(0) = children(0).typeInfo()
    oiTypeInfo(1) = children(1).typeInfo()
    
    commonTypeInfo = initCompareType()
    if (commonTypeInfo == null) {
      // can not be converted, still under control of CG (could handle it), but the value is
      // Constant Null, mark the UDF result is Constant Null
      markAsConstantNull()
      Array[ExprNode[ExprNodeDesc]]()
    } else {
      var commonOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(commonTypeInfo)
      v1 = ConverterNode(children(0), commonOI, initConvertType(0))
      v2 = ConverterNode(children(1), commonOI, initConvertType(1))
      
      Array(v1, v2)
    }
  }
  
  /**
   * Requires each parameter of UDF to be checked if is null, otherwise the result of UDF is null
   */
  override protected def requireNullValueCheck(parameterIndex: Int) = true

  override protected def cgUDFCall() = cgUDFCall(commonTypeInfo)
}

// Built-in NumericBased GenericUDF implementations
class UDFOPBaseNumericBinaryUDF(node: GenericFunNode)
  extends BinaryUDF(node) {
  override protected def cgUDFCallByte() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set((byte)(" + code + "));"
    else
      null
  }

  override protected def cgUDFCallShort() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set((short)(" + code + "));"
    else
      null
  }
  
  // we may need to override more cgUDFCallInt/Long/Double etc. but java could handle
  // the other primitive data type casting properly.
  
  override protected def initCompareType() = {
    // numeric-based built-in UDF only accept the java primitive numeric types
    // convert it to double if the argument is not in primitive numeric types.
    var compareType = super.initCompareType()
    if (compareType != TypeInfoFactory.byteTypeInfo &&
        compareType != TypeInfoFactory.shortTypeInfo &&
        compareType != TypeInfoFactory.intTypeInfo &&
        compareType != TypeInfoFactory.longTypeInfo &&
        compareType != TypeInfoFactory.floatTypeInfo &&
        compareType != TypeInfoFactory.doubleTypeInfo) {
      TypeInfoFactory.doubleTypeInfo
    } else {
      compareType
    }
  }
}

/**
 * +
 */
case class UDFOPPlusBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "+" + v2
}

/**
 * -
 */
case class UDFOPMinusBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "-" + v2
}

/**
 * *
 */
case class UDFOPMultiplyBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "*" + v2
}

/**
 * /
 */
case class UDFOPDivideBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def initCompareType() = {
      TypeInfoFactory.doubleTypeInfo
  }
  override protected def cgUDFCallPrimitive() = "(double)" + v1 + "/" + v2
}

/**
 * /
 */
case class UDFOPLongDivideBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def initCompareType() = {
      TypeInfoFactory.doubleTypeInfo
  }  
  override protected def cgUDFCallPrimitive() = "(double)" + v1 + "/" + v2
}

/**
 * %
 */
case class UDFOPModBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "%" + v2
}

/**
 * pmod ((a % b) + b) % b
 */
case class UDFPosModBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = 
    "((" + v1 + "%" + v2 + ") + " + v2 + ") % " + v2
}

/**
 * &
 */
case class UDFOPBitAndBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "&" + v2
}

/**
 * |
 */
case class UDFOPBitOrBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "|" + v2
}

/**
 * XOR
 */
case class UDFOPBitXorBinaryUDF(override val node: GenericFunNode)
  extends UDFOPBaseNumericBinaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1 + "^" + v2
}

class UDFOPLogical(override val node: GenericFunNode)
  extends BinaryUDF(node) {
    override protected def cgUDFCallBoolean() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set(" + code + ");"
    else
      null
  }
}

/**
 * AND
 */
case class UDFOPAnd(override val node: GenericFunNode)
  extends UDFOPLogical(node) {
  override protected def cgUDFCallPrimitive() = v1 + "&&" + v2
  // for AND, if the first(left) operand is false, then no need to calculate the right 
  override protected def partialEvaluateCheck(
    args: Array[_<:ExprCodeGen],
    currentParamIdx: Int) =
    if (currentParamIdx == 0) {
      "false == " + v1
    } else {
      null
    }

  override protected def partialEvaluateResult(
    args: Array[_<:ExprCodeGen],
    currentParamIdx: Int) =
    if (currentParamIdx == 0) {
      this.resultVariableName() + ".set(false)"
    } else {
      null
    }
}

/**
 * OR
 * the value is strict in the order of following rules
 * v1 != null && v1 == true ==> true
 * v2 != null && v2 == true ==> true
 * v1 != null && v2 != null ==> false
 * ==> null
 */
case class UDFOPOr(override val node: GenericFunNode)
  extends UDFOPLogical(node) {
    
  override def prepare(rowInspector: ObjectInspector, 
    children: Array[_<:ExprNode[ExprNodeDesc]]) = {
    var result = super.prepare(rowInspector, children)
    if (v1.constantNull() && v2.constantNull()) {
      markAsConstantNull()
      Array[ExprNode[ExprNodeDesc]]()
    } else {
      result
    }
  }
  
  /**
   * the partial evaluating is trying to figure out the value for
   * v1 != null && v1 == true ==> true
   * v2 != null && v2 == true ==> true
   */
  override protected def requireNullValueCheck(parameterIndex: Int) = {
    if(parameterIndex == 0 && v2.constantNull()) 
      true
    else if (parameterIndex == 1 && v1.constantNull()) 
      true
    else
      false
  }
  override protected def cgUDFCallPrimitive() = format(v1, v2)

  // for OR, if the first(left) operand is true, then no need to calculate the other 
  override protected def partialEvaluateCheck(
    args: Array[_<:ExprCodeGen],
    currentParamIdx: Int) =
    if (currentParamIdx == 0 && !v1.constantNull()) 
      partialCheck(0, v1)
    else if (currentParamIdx == 1 && !v2.constantNull())
      partialCheck(1, v2)
    else
      null

  override protected def partialEvaluateResult(
    args: Array[_<:ExprCodeGen],
    currentParamIdx: Int) = this.resultVariableName() + ".set(true)"
  
  private def partialCheck(idx: Int, node: ExprNode[_]) = {
     var cgValidate = node.cgValidateCheck()
     
     if((cgValidate == null) || requireNullValueCheck(idx))
        "" + node
      else {
        node.notifycgValidateCheckCodeNeed()
        cgValidate + "&&" + node
      }
  }
  
  private def format(
    v1: ExprNode[ExprNodeDesc],
    v2: ExprNode[ExprNodeDesc]) = {

    var check1 = v1.cgValidateCheck()
    var check2 = v2.cgValidateCheck()
    
    if (check1 != null) v1.notifycgValidateCheckCodeNeed()
    if (check2 != null) v2.notifycgValidateCheckCodeNeed()

    if (check1 != null && check2 != null) {
      "(%s && %s) ? false : (%s)".format(check1, check2, invalidValueExpr())
    } else if (check1 != null) { // v2 is not null constantly
      "(%s) ? %s : %s".format(check1, v1 + "||" + v2, v2)
    } else if (check2 != null) { // v1 is not null constantly
      "(%s) ? %s : %s".format(check2, v1 + "||" + v2, v1)
    } else { // both v1, v2 are not null constantly
      v1 + "||" + v2
    }
  }
}

/**
 * base compared
 */
abstract class UDFBaseCompare(override val node: GenericFunNode)
  extends BinaryUDF(node) {
}

/**
 * <
 */
case class UDFOPLessThan(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
  override protected def cgUDFCallText() =
    "ShimLoader.getHadoopShims().compareText(" + 
    v1.resultVariableName() + 
    "," + 
    v2.resultVariableName() + 
    ") < 0"
  override protected def cgUDFCallPrimitive() = v1 + "<" + v2
  override protected def cgUDFCallComparable() = 
    v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") < 0"
}

/**
 * <=
 */
case class UDFOPEqualOrLessThan(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
  override protected def cgUDFCallText() =
    "ShimLoader.getHadoopShims().compareText(" + 
    v1.resultVariableName() + 
    "," + 
    v2.resultVariableName() + 
    ") <= 0"
  override protected def cgUDFCallPrimitive() = v1 + "<=" + v2
  override protected def cgUDFCallComparable() = 
    v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") <= 0"
}

/**
 * >
 */
case class UDFOPGreaterThan(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
  override protected def cgUDFCallText() =
    "ShimLoader.getHadoopShims().compareText(" + 
    v1.resultVariableName() + 
    "," + 
    v2.resultVariableName() + 
    ") > 0"
  override protected def cgUDFCallPrimitive() = v1 + ">" + v2
  override protected def cgUDFCallComparable() = 
    v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") > 0"
}

/**
 * >=
 */
case class UDFOPEqualOrGreaterThan(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
  override protected def cgUDFCallText() =
    "ShimLoader.getHadoopShims().compareText(" + 
    v1.resultVariableName() + 
    "," + 
    v2.resultVariableName() + 
    ") >= 0"
  override protected def cgUDFCallPrimitive() = v1 + ">=" + v2
  override protected def cgUDFCallComparable() = 
    v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") >= 0"
}

/**
 * <=>
 */
case class UDFOPEqualNS(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {

  protected override def requireNullValueCheck(parameterIndex: Int) = false
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
    
  private def format(
    v1: ExprNode[ExprNodeDesc],
    v2: ExprNode[ExprNodeDesc],
    calcValue: String) = {
    var check1 = v1.codeValidationSnippet()
    var check2 = v2.codeValidationSnippet()
    
    if (v1.constantNull() && v2.constantNull()) {
      "true"
    } else if (v1.constantNull()) {
      if (check2 != null) {
        "(%s) ? false : true".format(check2)
      } else {
        "false"
      }
    } else if (v2.constantNull()) {
      if (check1 != null) {
        "(%s) ? false : true".format(check1)
      } else {
        "false"
      }
    } else {
      if (check1 != null && check2 != null) {
        "%s && %s ? (%s) : (((%s)||(%s)) ? false : true)".
          format(check1, check2, calcValue, check1, check2)
      } else if (check1 != null) { // v2 is not null constantly
        "(%s) ? (%s) : false".format(check1, calcValue)
      } else if (check2 != null) { // v1 is not null constantly
        "(%s) ? (%s) : false".format(check2, calcValue)
      } else { // v1, v2 are not null constantly
        calcValue
      }
    }
  }

 override protected def cgUDFCallBoolean() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set(" + code + ");"
    else
      null
  }
 
  override protected def cgUDFCallPrimitive() = 
    format(v1, v2, v1 + "==" + v2)
  override protected def cgUDFCallText() =
    format(v1, v2, "ShimLoader.getHadoopShims().compareText(" +
      v1.resultVariableName() + "," + v2.resultVariableName() + ") == 0")
  override protected def cgUDFCallComparable() =
    format(v1, v2, v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") == 0")
}

/**
 * ==
 */
case class UDFOPEqual(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
  override protected def cgUDFCallBoolean() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set(" + code + ");"
    else
      null
  }  
  override protected def cgUDFCallComparable() =
    v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") == 0"
  override protected def cgUDFCallPrimitive() =
    v1 + "==" + v2
  override protected def cgUDFCallText() =
    "ShimLoader.getHadoopShims().compareText(" + 
    v1.resultVariableName() + "," + v2.resultVariableName() + ") == 0"
}

/**
 * != / <>
 */
case class UDFOPNotEqual(override val node: GenericFunNode)
  extends UDFBaseCompare(node) {
  context.registerImport(classOf[org.apache.hadoop.hive.shims.ShimLoader])
  override protected def cgUDFCallPrimitive() =
    v1 + "!=" + v2
  override protected def cgUDFCallText() =
    "ShimLoader.getHadoopShims().compareText(" + 
    v1.resultVariableName() + 
    "," + 
    v2.resultVariableName() + 
    ") != 0"
  override protected def cgUDFCallComparable() =
    v1.resultVariableName() + ".compareTo(" + v2.resultVariableName() + ") != 0"
}
