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

import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.node.ExprNodeCodeGen
import shark.execution.cg.node.GenericFunExprNodeCodeGen
import shark.execution.cg.EvaluationType

sealed abstract class BinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFCodeGen(node) {
  object CompareType extends Enumeration {
    type CompareType = Value
    // Now only string, text, int, long, short, timestamp, byte and boolean comparisons are
    // treated as special cases.
    // For other types, we reuse ObjectInspectorUtils.compare()
    val UNKNOWN, VOID, STRING, TEXT, INT, LONG, SHORT, FLOAT, 
    DOUBLE, TIMESTAMP, BYTE, BINARY, BOOLEAN, SAME_TYPE = Value
  }

  import CompareType._
  protected var compareType : CompareType = _
  protected var typeConvert4First = false
  protected var typeConvert4Second = false

  initCompareType(node.getChildren().map(_.getOutputOI).toArray)

  override def evaluationType() = EvaluationType.SET

  override def invalidValueExpr() = nullValueIndicatorVariableName() + "=false"
  override def initValueExpr() = nullValueIndicatorVariableName() + "=true"

  /**
   * Requires very parameter to be checked if its value is null
   */
  override protected def requireNullValueCheck(parameterIndex : Int) = true

  private def getPrimitiveCategoryFromTypeInfo(typeInfo : TypeInfo) = {
    typeInfo match {
      case TypeInfoFactory.voidTypeInfo      => CompareType.VOID
      case TypeInfoFactory.booleanTypeInfo   => CompareType.BOOLEAN
      case TypeInfoFactory.intTypeInfo       => CompareType.INT
      case TypeInfoFactory.longTypeInfo      => CompareType.LONG
      case TypeInfoFactory.stringTypeInfo    => CompareType.STRING
      case TypeInfoFactory.floatTypeInfo     => CompareType.FLOAT
      case TypeInfoFactory.doubleTypeInfo    => CompareType.DOUBLE
      case TypeInfoFactory.byteTypeInfo      => CompareType.BYTE
      case TypeInfoFactory.shortTypeInfo     => CompareType.SHORT
      case TypeInfoFactory.timestampTypeInfo => CompareType.TIMESTAMP
      case TypeInfoFactory.binaryTypeInfo    => CompareType.BINARY
      case TypeInfoFactory.unknownTypeInfo   => CompareType.UNKNOWN
      case _                                 => throw new CGAssertRuntimeException("Cannot mapping the " + typeInfo)
    }
  }

  private def initCompareType(arguments : Array[ObjectInspector]) {
    var oiTypeInfo0 = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments(0))
    var oiTypeInfo1 = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments(1))

    if (oiTypeInfo0 != oiTypeInfo1) {
      var commonTypeInfo = FunctionRegistry.getCommonClass(oiTypeInfo0, oiTypeInfo1)

      // For now, we always convert to double if we can't find a common type
      if (commonTypeInfo == null) {
        commonTypeInfo = TypeInfoFactory.doubleTypeInfo
      }
      compareType = getPrimitiveCategoryFromTypeInfo(commonTypeInfo)

      if (oiTypeInfo0 != commonTypeInfo) {
        this.typeConvert4First = true
      }

      if (oiTypeInfo1 != commonTypeInfo) {
        this.typeConvert4Second = true
      }
    } else {
      if (oiTypeInfo0 == TypeInfoFactory.stringTypeInfo) {
        if ((arguments(0).asInstanceOf[StringObjectInspector]).preferWritable() ||
          (arguments(1).asInstanceOf[StringObjectInspector]).preferWritable()) {
          compareType = CompareType.TEXT;
        } else {
          compareType = CompareType.STRING;
        }
      } else {
        compareType = getPrimitiveCategoryFromTypeInfo(oiTypeInfo0);
      }
    }
  }

  override protected def cgUDFCall(nodes : Array[ExprNodeCodeGen[ExprNodeDesc]]) = {
    var v1 = nodes(0)
    var v2 = nodes(1)

    compareType match {
      case VOID => cgUDFCall_VOID(v1, v2)
      case UNKNOWN =>
        // TODO return cgUDFCall_UNKNOWN(v1, v2, udfVariableName);
        throw new CGAssertRuntimeException("UNKNOWN(Map/List/Union etc.) " +
        		"type binary function calls haven't been implemented yet.")
      case BOOLEAN   => cgUDFCall_BOOLEAN(v1, v2)
      case BYTE      => cgUDFCall_BYTE(v1, v2)
      case STRING    => cgUDFCall_STRING(v1, v2)
      case TEXT      => cgUDFCall_STRING(v1, v2)
      case SHORT     => cgUDFCall_SHORT(v1, v2)
      case INT       => cgUDFCall_INT(v1, v2)
      case LONG      => cgUDFCall_LONG(v1, v2)
      case FLOAT     => cgUDFCall_FLOAT(v1, v2)
      case DOUBLE    => cgUDFCall_DOUBLE(v1, v2)
      case TIMESTAMP => cgUDFCall_TIMESTAMP(v1, v2)
      case BINARY    => cgUDFCall_BINARY(v1, v2)
      case _         => throw new CGAssertRuntimeException("More compare type should be added.")
    }
  }

  protected def cgUDFCall_VOID(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) : String = 
    throw new UnsupportedOperationException("BinaryUDFCodeGen.cgUDFCall_VOID")
  protected def cgUDFCall_UNKNOWN(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) : String = 
    throw new UnsupportedOperationException("BinaryUDFCodeGen.cgUDFCall_UNKNOWN")
  protected def cgUDFCall_TEXT(v1 : String, v2 : String) : String = 
    throw new UnsupportedOperationException("BinaryUDFCodeGen.cgUDFCall_TEXT")
  protected def cgUDFCall_TEXT(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) : String = 
    cgUDFCall_TEXT(getExprValue(v1), getExprValue(v2))
  protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) : String = 
    throw new UnsupportedOperationException("BinaryUDFCodeGen.cgUDFCall_NUMBER")
  protected def cgUDFCall_PRIMITIVE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) : String = 
    cgUDFCall_PRIMITIVE(getExprValue(v1), getExprValue(v2))
  protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) : String = 
    throw new UnsupportedOperationException("BinaryUDFCodeGen.cgUDFCall_NUMBER")
  protected def cgUDFCall_COMPARABLE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) : String = 
    cgUDFCall_COMPARABLE(getExprValue(v1), getExprValue(v2))

  protected def cgUDFCall_BOOLEAN(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_BYTE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_SHORT(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_INT(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_LONG(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_DOUBLE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_FLOAT(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_PRIMITIVE(v1, v2) + ");"
  protected def cgUDFCall_TIMESTAMP(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_COMPARABLE(v1.resultVariableName(), v2.resultVariableName()) + ");"
  protected def cgUDFCall_BINARY(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = 
    resultVariableName() + ".set(" + cgUDFCall_COMPARABLE(v1.resultVariableName(), v2.resultVariableName()) + ");"
  protected def cgUDFCall_STRING(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) = {
    compareType match {
      case CompareType.TEXT | CompareType.STRING  => {
          var p1 = if (this.typeConvert4First) convertToText(v1.resultVariableName()) else v1.resultVariableName()
          var p2 = if (this.typeConvert4Second) convertToText(v2.resultVariableName()) else v2.resultVariableName()
          resultVariableName() + ".set(" + cgUDFCall_TEXT(p1, p2) + ");"
        }
//      case CompareType.STRING => 
//        resultVariableName() + ".set(" + cgUDFCall_COMPARABLE(v1, v2) + ");"
      case _                  => 
        throw new CGAssertRuntimeException("should be CompareType.TEXT or CompareType.STRING")
    }
  }
  
  protected def convertToText(v:String) = "new org.apache.hadoop.io.Text(String.valueOf(%s))".format(v)
}

class UDFOPBaseNumericBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_BYTE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) =
    resultVariableName() + ".set((byte)(" + cgUDFCall_PRIMITIVE(getExprValue(v1), getExprValue(v2)) + "));"

  override protected def cgUDFCall_SHORT(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) =
    resultVariableName() + ".set((short)(" + cgUDFCall_PRIMITIVE(getExprValue(v1), getExprValue(v2)) + "));"
}

class UDFOPPlusBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "+" + v2
}

class UDFOPMinusBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "-" + v2
}

class UDFOPMultiplyBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "*" + v2
}

class UDFOPDivideBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "/" + v2
}

class UDFOPLongDivideBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "/" + v2
}

class UDFOPModBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "%" + v2
}

class UDFPosModBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = 
    "((" + v1 + "%" + v2 + ") + " + v2 + ") % " + v2
}

class UDFOPBitAndBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "&" + v2
}

class UDFOPBitOrBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "|" + v2
}

class UDFOPBitXorBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFOPBaseNumericBinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "^" + v2
}

class GenericUDFOPAndCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "&&" + v2
  override protected def partialEvaluateCheck(args : Array[ExprNodeCodeGen[ExprNodeDesc]], currentParamIdx : Int) =
    if (currentParamIdx == 0) {
      "false == " + this.getExprValue(args(0))
    } else {
      null
    }

  override protected def partialEvaluateResult(args : Array[ExprNodeCodeGen[ExprNodeDesc]], currentParamIdx : Int) =
    if (currentParamIdx == 0) {
      this.resultVariableName() + ".set(false)"
    } else {
      null
    }
}

class GenericUDFOPOrCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "||" + v2
  override protected def partialEvaluateCheck(args : Array[ExprNodeCodeGen[ExprNodeDesc]], currentParamIdx : Int) =
    if (currentParamIdx == 0) "true == " + this.getExprValue(args(0)) else null

  override protected def partialEvaluateResult(args : Array[ExprNodeCodeGen[ExprNodeDesc]], currentParamIdx : Int) =
    if (currentParamIdx == 0) this.resultVariableName() + ".set(true)" else null
}

class GenericUDFOPLessThanCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_TEXT(v1 : String, v2 : String) =
    "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + v1 + "," + v2 + ") < 0"
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "<" + v2
  override protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) = v1 + ".compareTo(" + v2 + ") < 0)"
}

class GenericUDFOPEqualOrLessThanCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_TEXT(v1 : String, v2 : String) =
    "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + v1 + "," + v2 + ") <= 0"
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + "<=" + v2
  override protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) = v1 + ".compareTo(" + v2 + ") <= 0)"
}

class GenericUDFOPGreaterThanCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_TEXT(v1 : String, v2 : String) =
    "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + v1 + "," + v2 + ") > 0"
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + ">" + v2
  override protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) = v1 + ".compareTo(" + v2 + ") > 0)"
}

class GenericUDFOPEqualOrGreaterThanCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_TEXT(v1 : String, v2 : String) =
    "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + v1 + "," + v2 + ") >= 0"
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) = v1 + ">=" + v2
  override protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) = v1 + ".compareTo(" + v2 + ") >= 0)"
}

class GenericUDFOPEqualNSCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  this.setCgValidateCheck(null)

  private def format(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc], calcValue : String) = {
    var check1 = v1.cgValidateCheck()
    var check2 = v2.cgValidateCheck()

    if (check1 != null && check2 != null) {
      check1 + "&&" + check2 + "? (" + calcValue + ") : (((" + check1 + ")||(" + check2 + ")) ? false : true)"
    } else if (check1 != null) {
      "(" + check1 + ") ? false : true"
    } else if (check2 != null) {
      "(" + check2 + ") ? false : true"
    } else {
      "true"
    }
  }

  override protected def cgUDFCall_PRIMITIVE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) =
    format(v1, v2, getExprValue(v1) + "==" + getExprValue(v2))
  override protected def cgUDFCall_TEXT(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) =
    format(v1, v2, "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + 
        getExprValue(v1) + "," + getExprValue(v2) + ") == 0")
  override protected def cgUDFCall_COMPARABLE(v1 : ExprNodeCodeGen[ExprNodeDesc], v2 : ExprNodeCodeGen[ExprNodeDesc]) =
    format(v1, v2, getExprValue(v1) + ".compareTo(" + getExprValue(v2) + ") == 0")
  override protected def requireNullValueCheck(parameterIndex : Int) = false
  override def invalidValueExpr() : String = null
  override def initValueExpr() : String = null
}

class GenericUDFOPEqualCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) =
    v1 + ".compareTo(" + v2 + ") == 0"
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) =
    v1 + "==" + v2
  override protected def cgUDFCall_TEXT(v1 : String, v2 : String) =
    "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + v1 + "," + v2 + ") == 0"
}

class GenericUDFOPNotEqualCodeGen(node : GenericFunExprNodeCodeGen) 
  extends BinaryUDFCodeGen(node) {
  override protected def cgUDFCall_PRIMITIVE(v1 : String, v2 : String) =
    v1 + "!=" + v2
  override protected def cgUDFCall_TEXT(v1 : String, v2 : String) =
    "org.apache.hadoop.hive.shims.ShimLoader.getHadoopShims().compareText(" + v1 + "," + v2 + ") != 0"
  override protected def cgUDFCall_COMPARABLE(v1 : String, v2 : String) =
    v1 + ".compareTo(" + v2 + ") != 0"
}
