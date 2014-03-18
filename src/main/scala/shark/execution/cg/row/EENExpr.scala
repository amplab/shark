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

package shark.execution.cg.row

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable

import org.apache.spark.Logging
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf._
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.{ TypeInfoFactory => TIF }
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector

import shark.execution.cg.CGUtil
import shark.execution.cg.{ CGAssertRuntimeException, CGNotSupportDataTypeRuntimeException }
import shark.execution.cg.SetDeferred

/**
 * The generated java source class property definition
 */
class PropertyDefinition(
    val defType: String,
    val createInstance: Boolean,
    val isFinal: Boolean = false,
    val isStatic: Boolean = false,
    val initString: String = null,
    val defName: String = CGUtil.randProperty()) {

  final override def hashCode = 12 * defName.hashCode + 17 * defType.hashCode

  final override def equals(other: Any) = {
    val that = other.asInstanceOf[PropertyDefinition]
    if (that == null) false
    else defName == that.defName && defType == that.defType
  }

  override def toString() = {
    if (createInstance) // if required to create the field instance when declare the field
      "private %s%s %s %s %s;".format(
        if (isFinal) "final " else "",
        if (isStatic) "static " else "",
        defType,
        defName,
        if (initString == null) ("= new " + defType + "()") else ("= " + initString))
    else // if not required to create the field instance when declare the field
      "private %s%s %s;".format(
        if (isStatic) "static " else "",
        defType,
        defName)
  }
}

/**
 * Register the TEN (Typed Expression Node).
 * Each TEN will be associated with:
 * 1) expression variable name in generated source code
 * 2) expression variable type in generated source code
 * 3) expression validity (null check) in generated source code
 * 4) expression value representing in generated source code (sometimes is variable name, sometimes
 *      maybe its computing literal)
 * 5) invalid the expression value in generated source code (changing its indicator value)
 * 6) valid the expression value in generated source code (changing its indicator value)
 * Those lookup table value is in String, null represents the rule is not applicable.
 */
trait ExprSymbolLookUp {
  private val table = Map[TypedExprNode, Map[String, String]]()
  var row: TENOutputRow = _

  private def name(expr: TypedExprNode, prefix: String): String = prefix + expr.exprId

  def getExprCode(expr: TypedExprNode, key: String) = table.getOrElse(expr, {
    throw new CGAssertRuntimeException("unregistered expr " + expr)
  }).getOrElse(key, null)

  def register(node: TypedExprNode, dataType: String, codeIsValid: String, 
    codeValueRepl: String, codeInvalidate: String, codeValidate: String) {
    val entry = table.getOrElseUpdate(node, Map[String, String]())
    entry += (Constant.CODE_IS_VALID -> codeIsValid)
    entry += (Constant.EXPR_VARIABLE_TYPE -> dataType)
    entry += (Constant.CODE_VALUE_REPL -> codeValueRepl)
    entry += (Constant.CODE_INVALIDATE -> codeInvalidate)
    entry += (Constant.CODE_VALIDATE -> codeValidate)
  }

  def register(node: TypedExprNode, key: String, value: String) {
    table.getOrElseUpdate(node, Map[String, String]()) += (key -> value)
  }

  def register(node: TypedExprNode) {
    val entry = table.getOrElseUpdate(node, Map[String, String]())
    if (!entry.contains(Constant.EXPR_VARIABLE_NAME)) {
      entry += (Constant.EXPR_VARIABLE_NAME -> name(node, "__expr_"))
    }
    if (!entry.contains(Constant.EXPR_NULL_INDICATOR_NAME)) {
      entry += (Constant.EXPR_NULL_INDICATOR_NAME -> name(node, "__indicator_"))
    }
  }

  def codeIsValid(expr: TypedExprNode): String = getExprCode(expr, Constant.CODE_IS_VALID)
  def codeValueRepl(expr: TypedExprNode): String = getExprCode(expr, Constant.CODE_VALUE_REPL)
  def codeInvalidate(expr: TypedExprNode): String = getExprCode(expr, Constant.CODE_INVALIDATE)
  def codeValidate(expr: TypedExprNode): String = getExprCode(expr, Constant.CODE_VALIDATE)
  def exprName(expr: TypedExprNode): String = getExprCode(expr, Constant.EXPR_VARIABLE_NAME)
  def exprType(expr: TypedExprNode): String = getExprCode(expr, Constant.EXPR_VARIABLE_TYPE)
  def indicatorName(expr: TypedExprNode): String = getExprCode(expr, Constant.EXPR_NULL_INDICATOR_NAME)
  def indicatorDefaultValue(expr: TypedExprNode): String = 
    getExprCode(expr, Constant.EXPR_NULL_INDICATOR_DEFAULT_VALUE)
  def exprDefaultValue(expr: TypedExprNode, default: String): String = {
    val defValue = getExprCode(expr, Constant.EXPR_DEFAULT_VALUE)
    if (defValue == null) default else defValue
  }
}

/**
 * Generated Source Code Context, defines the import / class properties / and initializing code 
 * snippets.
 */
class CGExprContext extends ExprSymbolLookUp {
  val imports = scala.collection.mutable.Set[String]()
  val properties = ArrayBuffer[PropertyDefinition]()
  val initials = ArrayBuffer[String]()

  def defineImport(clazz: Class[_]) {
    imports += (clazz.getCanonicalName())
  }

  def defineImport(clazz: String) {
    imports += (clazz)
  }

  def addInitials(entry: String) {
    initials += (entry)
  }

  def property(defType: String, isCreate: Boolean = true, isFinal: Boolean = false, 
    initString: String = null, isStatic: Boolean = false): String = {
    val pd = new PropertyDefinition(defType, isCreate, isFinal, isStatic, initString)
    properties += (pd)

    pd.defName
  }

}

/**
 * Root class node of expression in source code generating tree 
 */
abstract class EENExpr(val ten: TypedExprNode, nested: ExecuteOrderedExprNode = null)
    extends ExecuteOrderedExprNode(nested) {
  self: Product =>

  override def currCode(ctx: CGExprContext): String = {
    val codeCompute = exprCode(ctx)

    val code = new StringBuffer()
    if (codeCompute != null && !codeCompute.isEmpty()) {
      code.append("%s = %s;".format(ctx.exprName(ten), codeCompute))
    }

    val validate = ctx.codeValidate(ten)
    if (validate != null) {
      code.append(validate)
    }

    code.toString()
  }

  override def initialEssential(ctx: CGExprContext) {
    register(ctx, essential)

    initial(ctx)
  }

  protected def register(ctx: CGExprContext, ten: TypedExprNode) {
    if (ten != null) {
      ctx.register(ten)
      ctx.register(ten,
        ten.outputDT.primitive,
        ctx.indicatorName(ten),
        ctx.exprName(ten),
        "%s = false;".format(ctx.indicatorName(ten)),
        "%s = true;".format(ctx.indicatorName(ten)))
      ctx.register(ten, Constant.EXPR_NULL_INDICATOR_DEFAULT_VALUE, "false")
    }
  }

  protected def initial(ctx: CGExprContext) {}

  protected def exprCode(ctx: CGExprContext): String = ""

  override def essential: TypedExprNode = ten
}

case class EENAlias(expr: TypedExprNode, sibling: ExecuteOrderedExprNode = null) 
    extends EENExpr(expr, sibling) {
  override def initialEssential(ctx: CGExprContext) {
    // do nothing rather than its default initialization.
  }

  override def currCode(ctx: CGExprContext): String = ""
}

case class EENInputRow(expr: TENInputRow, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  private var oiName: String = _
  private var sfName: String = _

  override def initial(ctx: CGExprContext) {
    import org.apache.hadoop.hive.serde2.objectinspector.StructField
    //    val oiType = TypeUtil.dtToTypeOIString(expr.outputDT)
    val oiType = TypeUtil.dtToString(expr.outputDT)

    oiName = ctx.property(oiType, false, false, null, false)
    sfName = ctx.property(classOf[StructField].getCanonicalName(), false, false, null, false)

    ctx.addInitials("%s = %s.getStructFieldRef(\"%s\");".format(sfName,
      Constant.CG_EXPR_NAME_INPUT_SOI, expr.attr))
    ctx.addInitials("%s = (%s)(%s.getFieldObjectInspector());".format(oiName, oiType, sfName))

    ctx.register(expr, Constant.CODE_VALUE_REPL, ctx.exprName(expr))

    if (expr.outputWritable) {
      ctx.register(expr, Constant.CODE_INVALIDATE, null)
      ctx.register(expr, Constant.CODE_VALIDATE, null)
      ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
      ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
      ctx.register(expr, Constant.CODE_IS_VALID, "%s != null".format(ctx.exprName(expr)))

      // TODO need to thing about the union / map / array / structure type 
      ctx.register(expr, Constant.EXPR_DEFAULT_VALUE,
        "(%s)%s.getPrimitiveWritableObject(%s)".format(
          expr.outputDT.writable,
          oiName, exprData))
    } else {
      ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.outputDT.primitive)
    }
  }

  override def currCode(ctx: CGExprContext): String = {
    if (expr.outputWritable) {
      super.currCode(ctx)
    } else {
      """{
          Object tempobj = %s; 
            if(tempobj != null) {
            %s = %s;
            %s
            }
           }""".format(exprData, ctx.exprName(expr), exprJavaData, ctx.codeValidate(ten))
    }
  }

  private def exprData = {
    "%s.getStructFieldData(%s, %s)".format(
      Constant.CG_EXPR_NAME_INPUT_SOI,
      Constant.CG_EXPR_NAME_INPUT,
      sfName)
  }

  private def exprJavaData: String = {
    expr.outputDT match {
      case x: CGPrimitiveString => "%s.getPrimitiveJavaObject(tempobj)".format(oiName)
      case x: CGPrimitiveTimestamp => "%s.getPrimitiveJavaObject(tempobj)".format(oiName)
      case x: CGPrimitiveBinary => "%s.getPrimitiveJavaObject(tempobj)".format(oiName)
      case _ => "%s.get(tempobj)".format(oiName)
    }
  }
}

case class EENCondition(predict: ExecuteOrderedExprNode, t: ExecuteOrderedExprNode, 
  f: ExecuteOrderedExprNode, branch: TENBranch, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(branch, sibling) {
  override def currCode(ctx: CGExprContext): String = {
    val pc = ctx.exprName(predict.essential)
    val tc = t.code(ctx)
    val fc = f.code(ctx)
    "if(%s) \n{%s\n} else {%s\n}".format(pc, tc, fc)
  }

  override def children = (predict :: t :: f :: sibling :: Nil).filter(_ != null)
}

case class EENOutputField(output: TENOutputField, outter: ExecuteOrderedExprNode = null) 
    extends EENExpr(output) {
  self: Product =>

  override def initial(ctx: CGExprContext) {
    ctx.register(output.expr)
    ctx.register(output, Constant.CODE_VALIDATE, "%s.mask.set(%s.%s, true);".format(
      Constant.CG_EXPR_NAME_OUTPUT, ctx.row.output.clazz, output.maskBitName))
    ctx.register(output, Constant.EXPR_VARIABLE_NAME, "%s.%s".format(
      Constant.CG_EXPR_NAME_OUTPUT, output.escapedName))
    ctx.register(output, Constant.CODE_VALUE_REPL, "%s.%s".format(
      Constant.CG_EXPR_NAME_OUTPUT, output.escapedName))
  }

  override def exprCode(ctx: CGExprContext): String = ctx.exprName(outter.essential)
}

case class EENOutputWritableField(output: TENOutputWritableField, 
  outter: ExecuteOrderedExprNode = null) extends EENExpr(output) {
  self: Product =>

  override def initial(ctx: CGExprContext) {
    ctx.register(output.expr)
    ctx.register(output, Constant.CODE_VALIDATE, null)
    ctx.register(output, Constant.EXPR_VARIABLE_NAME, "%s[%s]".format(
      Constant.CG_EXPR_NAME_OUTPUT, output.fieldIdx))
    ctx.register(output, Constant.CODE_VALUE_REPL, "%s[%s]".format(
      Constant.CG_EXPR_NAME_OUTPUT, output.fieldIdx))
  }

  override def exprCode(ctx: CGExprContext): String = ctx.exprName(outter.essential)
}

case class EENOutputExpr(output: TENOutputExpr, een: ExecuteOrderedExprNode) 
    extends EENExpr(output) {
  override def initial(ctx: CGExprContext) {
    ctx.register(output, Constant.EXPR_VARIABLE_NAME, Constant.CG_EXPR_NAME_OUTPUT)
    ctx.register(output, Constant.EXPR_NULL_INDICATOR_NAME, null)
    ctx.register(output, Constant.CODE_IS_VALID, null)
    ctx.register(output, Constant.CODE_INVALIDATE, null)
    ctx.register(output, Constant.EXPR_VARIABLE_TYPE, null)
    ctx.register(output, Constant.CODE_VALIDATE, null)
  }

  override def exprCode(ctx: CGExprContext): String = ctx.exprName(een.essential)
}

case class EENOutputRow(expr: TENOutputRow, een: ExecuteOrderedExprNode) 
    extends ExecuteOrderedExprNode(een) {
  override def initialEssential(ctx: CGExprContext) {
    ctx.register(expr, Constant.CODE_INVALIDATE, null)
    ctx.row = expr
  }

  override def code(ctx: CGExprContext): String = een.code(ctx)

  override def essential: TypedExprNode = expr
}
case class EENAttribute(expr: TENAttribute, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  
  override def exprCode(ctx: CGExprContext) = 
    """%s.%s""".format(ctx.exprName(expr.child), expr.escapedName)
}

case class EENLiteral(expr: TENLiteral, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  override def initial(ctx: CGExprContext) {
    TypeUtil.assertDataType(expr.outputDT)
    if (expr.dt != TypeUtil.NullType) {
      val variableName = expr.obj match {
        case null => if (expr.writable) {
          ctx.property(expr.dt.writable, false, true, null, true)
        } else {
          ctx.property(expr.dt.primitive, false, true, null, true)
        }
        case x: NullWritable => "null"
        case x: Text => {
          val bytes = textConvert2ByteArrayInHex(x)
          val t = if (expr.writable) classOf[Text].getCanonicalName() else "String"
          val v: String = if (expr.writable) string2Text(bytes) else bytes
          ctx.property(t, true, true, v, true)
        }
        case x: String => {
          val bytes = textConvert2ByteArrayInHex(x)
          val t = if (expr.writable) classOf[Text].getCanonicalName() else "String"
          val v: String = if (expr.writable) string2Text(bytes) else bytes
          ctx.property(t, true, true, v, true)
        }
        case x: BytesWritable => {
          val t = if (expr.writable) classOf[BytesWritable].getCanonicalName() else "byte[]"
          val v: String = if (expr.writable) bytes2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: Array[Byte] => {
          val t = if (expr.writable) classOf[BytesWritable].getCanonicalName() else "byte[]"
          val v: String = if (expr.writable) bytes2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: IntWritable => {
          val t = if (expr.writable) classOf[IntWritable].getCanonicalName() else "int"
          val v: String = if (expr.writable) int2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Integer => {
          val t = if (expr.writable) classOf[IntWritable].getCanonicalName() else "int"
          val v: String = if (expr.writable) int2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: BooleanWritable => {
          val t = if (expr.writable) classOf[BooleanWritable].getCanonicalName() else "boolean"
          val v: String = if (expr.writable) boolean2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Boolean => {
          val t = if (expr.writable) classOf[BooleanWritable].getCanonicalName() else "boolean"
          val v: String = if (expr.writable) boolean2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: FloatWritable => {
          val t = if (expr.writable) classOf[FloatWritable].getCanonicalName() else "float"
          val v: String = if (expr.writable) float2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Float => {
          val t = if (expr.writable) classOf[FloatWritable].getCanonicalName() else "float"
          val v: String = if (expr.writable) float2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: DoubleWritable => {
          val t = if (expr.writable) classOf[DoubleWritable].getCanonicalName() else "double"
          val v: String = if (expr.writable) double2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Double => {
          val t = if (expr.writable) classOf[DoubleWritable].getCanonicalName() else "double"
          val v: String = if (expr.writable) double2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: LongWritable => {
          val t = if (expr.writable) classOf[LongWritable].getCanonicalName() else "long"
          val v: String = if (expr.writable) long2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Long => {
          val t = if (expr.writable) classOf[LongWritable].getCanonicalName() else "long"
          val v: String = if (expr.writable) long2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: ByteWritable => {
          val t = if (expr.writable) classOf[ByteWritable].getCanonicalName() else "byte"
          val v: String = if (expr.writable) byte2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Byte => {
          val t = if (expr.writable) classOf[ByteWritable].getCanonicalName() else "byte"
          val v: String = if (expr.writable) byte2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: ShortWritable => {
          val t = if (expr.writable) classOf[ShortWritable].getCanonicalName() else "short"
          val v: String = if (expr.writable) short2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.lang.Short => {
          val t = if (expr.writable) classOf[ShortWritable].getCanonicalName() else "short"
          val v: String = if (expr.writable) short2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: TimestampWritable => {
          val t = if (expr.writable) {
            classOf[TimestampWritable].getCanonicalName()
          } else {
            classOf[java.sql.Timestamp].getCanonicalName()
          }
          val v: String = if (expr.writable) timestamp2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case x: java.sql.Timestamp => {
          val t = if (expr.writable) {
            classOf[TimestampWritable].getCanonicalName()
          } else {
            classOf[java.sql.Timestamp].getCanonicalName()
          }
          val v: String = if (expr.writable) timestamp2Writable(x) else x
          ctx.property(t, true, true, v, true)
        }
        case _ => throw new CGAssertRuntimeException("TODO")
      }

      if (expr.writable) ctx.defineImport(expr.outputDT.writable)

      ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, null)
      ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
      ctx.register(expr, Constant.EXPR_VARIABLE_NAME, variableName)
      ctx.register(expr, Constant.CODE_VALUE_REPL, variableName)
      ctx.register(expr, Constant.CODE_VALIDATE, "")
      ctx.register(expr, Constant.CODE_IS_VALID, null)
    } else {
      // is constant null without type e.g: select key - null from src;
      ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, null)
      ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
      ctx.register(expr, Constant.EXPR_VARIABLE_NAME, null)
      ctx.register(expr, Constant.CODE_VALUE_REPL, null)
      ctx.register(expr, Constant.CODE_VALIDATE, "")
      ctx.register(expr, Constant.CODE_IS_VALID, "false")
    }
  }

  override def exprCode(ctx: CGExprContext) = null
}

case class EENUDF(expr: TENUDF, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
  private var udf: String = _

  override def initial(ctx: CGExprContext) {
    TypeUtil.assertDataType(expr.outputDT)
    udf = ctx.property(expr.bridge.getUdfClass().getCanonicalName())

    if (expr.outputWritable) {
      // if the output is writable
      ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
      ctx.register(expr, Constant.CODE_IS_VALID, "%s != null".format(ctx.exprName(expr)))
      ctx.register(expr, Constant.CODE_VALIDATE, "")
      ctx.register(expr, Constant.CODE_INVALIDATE, null)
      ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
    } else {
      ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.outputDT.primitive)
    }
  }

  override def exprCode(ctx: CGExprContext) = {
    "%s.evaluate(%s)".format(udf,
      if (expr.children.length == 0)
        ""
      else
        expr.children.map(ctx.exprName(_)).reduce((a, b) => a + "," + b))
  }
}

case class EENGUDF(expr: TENGUDF, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
  private var gudf: String = _

  override def initial(ctx: CGExprContext) {
    TypeUtil.assertDataType(expr.outputDT)
    ctx.defineImport(expr.genericUDF.getClass())
    ctx.defineImport(classOf[PrimitiveObjectInspectorFactory])
    ctx.defineImport(classOf[ObjectInspector])
    ctx.defineImport(classOf[DeferredObject])
    ctx.defineImport(classOf[SetDeferred])

    gudf = ctx.property(expr.genericUDF.getClass().getCanonicalName())

    // TODO
    val inits = "%s.initialize(new ObjectInspector[]{%s});".format(
      gudf,
      expr.children.map(x => TypeUtil.dtToString(x.outputDT)).reduce((a, b) => a + "," + b))

    ctx.addInitials(inits)
    ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
    ctx.register(expr, Constant.CODE_IS_VALID, "%s != null".format(ctx.exprName(expr)))
    ctx.register(expr, Constant.CODE_VALIDATE, "")
    ctx.register(expr, Constant.CODE_INVALIDATE, null)
    ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
  }

  override def exprCode(ctx: CGExprContext) = {
    // TODO should use the lazy computing for performance purpose, trigger the child node 
    // evaluating when it's called (properly we could do that in a coded DeferredObject)
    "(%s)%s.evaluate(new DeferredObject[]{%s})".format(
      expr.outputDT.writable,
      gudf,
      if (expr.children.length == 0)
        ""
      else
        expr.children.map(ctx.exprName(_)).reduce((a, b) => a + "," + b))
  }
}

case class EENBuiltin(expr: TENBuiltin, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  override def initial(ctx: CGExprContext) {
    if (expr.nullCheckRequired == false) {
      ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
      ctx.register(expr, Constant.CODE_IS_VALID, null)
      ctx.register(expr, Constant.CODE_INVALIDATE, null)
      ctx.register(expr, Constant.CODE_VALIDATE, "")
    }
  }

  override def exprCode(ctx: CGExprContext): String = if (expr.exprs.length == 1) {
    expr.op match {
      case TENBuiltin.OP_COND_ISNULL => "!(%s)".format(ctx.codeIsValid(expr.children(0)))
      case TENBuiltin.OP_COND_ISNOTNULL => "%s".format(ctx.codeIsValid(expr.children(0)))
      case TENBuiltin.OP_COND_ISTRUE => "%s".format(ctx.codeValueRepl(expr.children(0)))
      case TENBuiltin.OP_COND_ISFALSE => "!(%s)".format(ctx.codeValueRepl(expr.children(0)))
      case _ => "%s%s".format(expr.op, ctx.codeValueRepl(expr.children(0)))
    }
  } else {
    expr.op match {
      case TENBuiltin.OP_CMP_GREATE => expr.children(0).outputDT match {
        case TypeUtil.StringType => "%s.compareTo(%s) > 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.BinaryType => "org.apache.hadoop.io.WritableComparator.compareBytes(" + 
          "%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) > 0".format(
            ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.TimestampType => "%s.compareTo(%s) > 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case _ => defaultExpr(ctx)
      }
      case TENBuiltin.OP_CMP_GREATE_OR_EQUAL => expr.children(0).outputDT match {
        case TypeUtil.StringType => "%s.compareTo(%s) >= 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.BinaryType => "org.apache.hadoop.io.WritableComparator.compareBytes(" + 
          "%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) >= 0".format(
            ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.TimestampType => "%s.compareTo(%s) >= 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case _ => defaultExpr(ctx)
      }
      case TENBuiltin.OP_CMP_EQUAL => expr.children(0).outputDT match {
        case TypeUtil.StringType => "%s.equals(%s)".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.BinaryType => "org.apache.hadoop.io.WritableComparator.compareBytes(" + 
          "%1$s, 0, %1$s.length, %2$s, 0, %2$s.length).compareTo(%s) == 0".format(
            ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.TimestampType => "%s.equals(%s)".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case _ => defaultExpr(ctx)
      }
      case TENBuiltin.OP_CMP_NOT_EQUAL => expr.children(0).outputDT match {
        case TypeUtil.StringType => "!%s.equals(%s)".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.BinaryType => "org.apache.hadoop.io.WritableComparator.compareBytes(" +
          "%1$s, 0, %1$s.length, %2$s, 0, %2$s.length).compareTo(%s) != 0".format(
            ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.TimestampType => "!%s.equals(%s)".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case _ => defaultExpr(ctx)
      }
      case TENBuiltin.OP_CMP_LESS_OR_EQUAL => expr.children(0).outputDT match {
        case TypeUtil.StringType => "%s.compareTo(%s) <= 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.BinaryType => "org.apache.hadoop.io.WritableComparator.compareBytes(" + 
          "%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) <= 0".format(
            ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.TimestampType => "%s.compareTo(%s) <= 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case _ => defaultExpr(ctx)
      }
      case TENBuiltin.OP_CMP_LESS => expr.children(0).outputDT match {
        case TypeUtil.StringType => "%s.compareTo(%s) < 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.BinaryType => "org.apache.hadoop.io.WritableComparator.compareBytes(" + 
          "%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) < 0".format(
            ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case TypeUtil.TimestampType => "%s.compareTo(%s) < 0".format(
          ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
        case _ => defaultExpr(ctx)
      }
      case _ => defaultExpr(ctx)
    }
  }

  private def defaultExpr(ctx: CGExprContext): String = {
    val snippet = expr.exprs.map(ctx.exprName(_)).reduce((a, b) => { 
      "%s%s%s".format(a, expr.op.symbol, b) 
    })
    expr.outputDT match {
      case TypeUtil.ByteType => "(byte)(%s)".format(snippet)
      case TypeUtil.ShortType => "(short)(%s)".format(snippet)
      case _ => snippet
    }
  }
}

case class EENConvertR2R(expr: TENConvertR2R, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  override def currCode(ctx: CGExprContext): String = {
    val codeCompute = exprCode(ctx)

    val code = new StringBuffer()
    val dt = expr.from.outputDT
    // TODO should add one more EEN (throw catch)
    if (dt == TypeUtil.StringType) {
      code.append("try{");
    }
    code.append("%s = %s;".format(ctx.exprName(ten), codeCompute))

    val validate = ctx.codeValidate(ten)
    if (validate != null) {
      code.append(validate)
    }

    if (dt == TypeUtil.StringType) {
      code.append("} catch (Throwable ignore){}".format(ctx.exprName(ten), codeCompute))
    }

    code.toString()
  }

  override def exprCode(ctx: CGExprContext) = {
    val dtFrom = expr.from.outputDT
    val dtTo = expr.to

    if (dtFrom == dtTo) {
      ctx.exprName(expr.from)
    } else {
      dtTo match {
        case TypeUtil.StringType => convertToString(ctx, expr.from, dtFrom)
        case TypeUtil.BinaryType => convertToBinary(ctx, expr.from, dtFrom)
        case TypeUtil.IntegerType => convertToInt(ctx, expr.from, dtFrom)
        case TypeUtil.BooleanType => convertToBoolean(ctx, expr.from, dtFrom)
        case TypeUtil.FloatType => convertToFloat(ctx, expr.from, dtFrom)
        case TypeUtil.DoubleType => convertToDouble(ctx, expr.from, dtFrom)
        case TypeUtil.LongType => convertToLong(ctx, expr.from, dtFrom)
        case TypeUtil.ByteType => convertToByte(ctx, expr.from, dtFrom)
        case TypeUtil.ShortType => convertToShort(ctx, expr.from, dtFrom)
        case TypeUtil.TimestampType => convertToTimestamp(ctx, expr.from, dtFrom)
        case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert from " + dtFrom)
      }
    }
  }

  def convertToString(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.BinaryType => "new String(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.BooleanType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.FloatType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.DoubleType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.LongType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.ByteType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.ShortType => "String.valueOf(%s)".format(fromExprName)
      case TypeUtil.TimestampType => "%s.toString()".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to String")
    }
  }

  def convertToBinary(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "%s.getBytes()".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert binary")
    }
  }

  def convertToInt(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)

    dtFrom match {
      case TypeUtil.StringType => "Integer.parseInt(%s)".format(fromExprName)
      case TypeUtil.BooleanType => "%s ? 1 : 0".format(fromExprName)
      case TypeUtil.FloatType => "((int)%s)".format(fromExprName)
      case TypeUtil.DoubleType => "((int)%s)".format(fromExprName)
      case TypeUtil.LongType => "((int)%s)".format(fromExprName)
      case TypeUtil.ByteType => "((int)%s)".format(fromExprName)
      case TypeUtil.ShortType => "((int)%s)".format(fromExprName)
      case TypeUtil.TimestampType => "(int)(%s.getSeconds())".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Int")
    }
  }

  def convertToBoolean(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "Boolean.parseBoolean(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "(%s == 0 ? false : true)".format(fromExprName)
      case TypeUtil.FloatType => "(%s == 0 ? false : true)".format(fromExprName)
      case TypeUtil.DoubleType => "(%s == 0 ? false : true)".format(fromExprName)
      case TypeUtil.LongType => "(%s == 0 ? false : true)".format(fromExprName)
      case TypeUtil.ByteType => "(%s == 0 ? false : true)".format(fromExprName)
      case TypeUtil.ShortType => "(%s == 0 ? false : true)".format(fromExprName)
      case TypeUtil.TimestampType => "(%s.getTime() == 0 ? false : true)".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Boolean")
    }
  }

  def convertToFloat(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "Float.parseFloat(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "((float)%s)".format(fromExprName)
      case TypeUtil.BooleanType => "(%s ? 1.0f : 0.0f)".format(fromExprName)
      case TypeUtil.DoubleType => "((float)%s)".format(fromExprName)
      case TypeUtil.LongType => "((float)%s)".format(fromExprName)
      case TypeUtil.ByteType => "((float)%s)".format(fromExprName)
      case TypeUtil.ShortType => "((float)%s)".format(fromExprName)
      case TypeUtil.TimestampType => "((float)%s.getTime())".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Float")
    }
  }

  def convertToDouble(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "Double.parseDouble(%s)".format(fromExprName)
      case TypeUtil.IntegerType => fromExprName
      case TypeUtil.BooleanType => "(%s ? 1.0d : 0.0d)".format(fromExprName)
      case TypeUtil.FloatType => fromExprName
      case TypeUtil.LongType => fromExprName
      case TypeUtil.ByteType => fromExprName
      case TypeUtil.ShortType => fromExprName
      case TypeUtil.TimestampType => "%s.getTime()".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Double")
    }
  }

  def convertToLong(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "Long.parseLong(%s)".format(fromExprName)
      case TypeUtil.IntegerType => fromExprName
      case TypeUtil.BooleanType => "(%s ? 1l : 0l)".format(fromExprName)
      case TypeUtil.FloatType => "((long)%s)".format(fromExprName)
      case TypeUtil.DoubleType => "((long)%s)".format(fromExprName)
      case TypeUtil.ByteType => fromExprName
      case TypeUtil.ShortType => fromExprName
      case TypeUtil.TimestampType => "%s.getTime()".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Long")
    }
  }

  def convertToByte(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "Byte.parseByte(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "((byte)%s)".format(fromExprName)
      case TypeUtil.BooleanType => "(%s ? (byte)1 : (byte)0)".format(fromExprName)
      case TypeUtil.FloatType => "((byte)%s)".format(fromExprName)
      case TypeUtil.DoubleType => "((byte)%s)".format(fromExprName)
      case TypeUtil.LongType => "((byte)%s)".format(fromExprName)
      case TypeUtil.ShortType => "((byte)%s)".format(fromExprName)
      case TypeUtil.TimestampType => "((byte)(%s.getTime()))".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Byte")
    }
  }

  def convertToShort(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "Short.parseShort(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "((short)%s)".format(fromExprName)
      case TypeUtil.BooleanType => "(%s ? (short)1 : (short)0)".format(fromExprName)
      case TypeUtil.FloatType => "((short)%s)".format(fromExprName)
      case TypeUtil.DoubleType => "((short)%s)".format(fromExprName)
      case TypeUtil.LongType => "((short)%s)".format(fromExprName)
      case TypeUtil.ByteType => "((short)%s)".format(fromExprName)
      case TypeUtil.TimestampType => "((short)(%s.getTime()))".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Short")
    }
  }

  def convertToDate(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)

    dtFrom match {
      case TypeUtil.StringType => "java.util.Date.parse(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "new java.util.Date(%s)".format(fromExprName)
      case TypeUtil.BooleanType => 
        "(%s ? new java.util.Date(1l) : new java.util.Date(0l)".format(fromExprName)
      case TypeUtil.FloatType => "new java.util.Date(%s)".format(fromExprName)
      case TypeUtil.DoubleType => "new java.util.Date(%s)".format(fromExprName)
      case TypeUtil.LongType => "new java.util.Date(%s)".format(fromExprName)
      case TypeUtil.ByteType => "new java.util.Date(%s)".format(fromExprName)
      case TypeUtil.ShortType => "new java.util.Date(%s)".format(fromExprName)
      case TypeUtil.TimestampType => "new java.util.Date(%s.getTime())".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Date")
    }
  }

  def convertToTimestamp(ctx: CGExprContext, from: TypedExprNode, dtFrom: DataType) = {
    val fromExprName = ctx.exprName(from)
    dtFrom match {
      case TypeUtil.StringType => "java.sql.Timestamp.parse(%s)".format(fromExprName)
      case TypeUtil.IntegerType => "new java.sql.Timestamp(%s)".format(fromExprName)
      case TypeUtil.BooleanType => 
        "(%s ? new java.sql.Timestamp(1l) : new java.sql.Timestamp(0l)".format(fromExprName)
      case TypeUtil.FloatType => "new java.sql.Timestamp(%s)".format(fromExprName)
      case TypeUtil.DoubleType => "new java.sql.Timestamp(%s)".format(fromExprName)
      case TypeUtil.LongType => "new java.sql.Timestamp(%s)".format(fromExprName)
      case TypeUtil.ByteType => "new java.sql.Timestamp(%s)".format(fromExprName)
      case TypeUtil.ShortType => "new java.sql.Timestamp(%s)".format(fromExprName)
      case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Timestamp")
    }
  }

  override def initial(ctx: CGExprContext) {
    if (expr.from.isInstanceOf[TENLiteral]) {
      val from = expr.from.asInstanceOf[TENLiteral]
      if (from.obj != null) {
        from.outputDT match {
          case TypeUtil.StringType =>
          case TypeUtil.TimestampType =>
          case TypeUtil.BinaryType =>
          // TODO add more non-java primitive type here.
          case _ => resetNullIndicator(ctx)
        }
      }
    }
  }

  private def resetNullIndicator(ctx: CGExprContext) {
    ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
    ctx.register(expr, Constant.CODE_IS_VALID, null)
    ctx.register(expr, Constant.CODE_INVALIDATE, null)
    ctx.register(expr, Constant.CODE_VALIDATE, null)
  }
}

case class EENConvertR2W(expr: TENConvertR2W, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  var storageName: String = _

  override def initial(ctx: CGExprContext) {
    storageName = ctx.property(expr.outputDT.writable)

    ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
    ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
    ctx.register(expr, Constant.CODE_IS_VALID, "%s!=null".format(ctx.exprName(expr)))
    ctx.register(expr, Constant.CODE_INVALIDATE, "%s = null".format(ctx.exprName(expr)))
    ctx.register(expr, Constant.CODE_VALIDATE, "")
    ctx.register(expr, Constant.CODE_VALUE_REPL, ctx.exprName(expr))
  }

  override def exprCode(ctx: CGExprContext) = "%s.build(%s, %s)".format(
    TypeUtil.getSetWritableClass().getCanonicalName(), storageName, ctx.exprName(expr.from))
}

case class EENConvertW2R(expr: TENConvertW2R, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  override def initial(ctx: CGExprContext) {
    ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, expr.from.outputDT.primitive)
  }
  override def exprCode(ctx: CGExprContext) = expr.from.outputDT match {
    case TypeUtil.StringType => "%s.toString()".format(ctx.exprName(expr.from))
    case TypeUtil.BinaryType => "%s.getBytes(%s)".format(
      TypeUtil.getSetRawClass().getCanonicalName(), ctx.exprName(expr.from))
    case TypeUtil.TimestampType => "%s.getTimestamp()".format(ctx.exprName(expr.from))

    case _ => "%s.get()".format(ctx.exprName(expr.from))
  }
}

case class EENConvertW2D(expr: TENConvertW2D, sibling: ExecuteOrderedExprNode) 
    extends EENExpr(expr, sibling) {
  var deferredName: String = _

  override def initial(ctx: CGExprContext) {
    ctx.defineImport(TypeUtil.getDeferredObjectClass())
    deferredName = ctx.property(TypeUtil.getDeferredObjectClass().getCanonicalName())

    ctx.register(expr, Constant.EXPR_VARIABLE_TYPE, 
      TypeUtil.getDeferredObjectClass().getCanonicalName())
    ctx.register(expr, Constant.EXPR_NULL_INDICATOR_NAME, null)
    ctx.register(expr, Constant.CODE_IS_VALID, null)
    ctx.register(expr, Constant.CODE_VALIDATE, "")
  }

  override def exprCode(ctx: CGExprContext) = 
    "%s.build(%s)".format(deferredName, ctx.exprName(expr.from))
}

