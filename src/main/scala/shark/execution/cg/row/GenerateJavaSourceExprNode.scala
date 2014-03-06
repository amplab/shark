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

trait ExprSymbolLookUp {
	val CODE_IS_VALID = "code_is_valid"
	val CODE_VALUE_REPL = "code_value_repl"
	val CODE_INVALIDATE = "code_invalidate"
	val CODE_VALIDATE = "code_validate"
	val EXPR_VARIABLE_NAME = "expr_variable_name"
    val EXPR_VARIABLE_TYPE = "expr_varialbe_type"
	val EXPR_NULL_INDICATOR_NAME = "expr_null_indicator"
	val EXPR_NULL_INDICATOR_DEFAULT_VALUE = "expr_null_indicator_default_val"
	val EXPR_DEFAULT_VALUE = "expr_default_val"

	private val table = Map[TypedExprNode, Map[String, String]]()
	var row: TENOutputRow = _
	
	private def name(expr: TypedExprNode, prefix: String): String = prefix + expr.exprId

	def getExprCode(expr: TypedExprNode, key: String) = table.getOrElse(expr, {
		throw new CGAssertRuntimeException("unregistered expr " + expr)
	}).getOrElse(key, null)

	def register(node: TypedExprNode, dataType: String, codeIsValid: String, codeValueRepl: String, codeInvalidate: String, codeValidate: String) {
		val entry = table.getOrElseUpdate(node, Map[String, String]())
		entry += (CODE_IS_VALID -> codeIsValid)
		entry += (EXPR_VARIABLE_TYPE -> dataType)
		entry += (CODE_VALUE_REPL -> codeValueRepl)
		entry += (CODE_INVALIDATE -> codeInvalidate)
		entry += (CODE_VALIDATE -> codeValidate)
	}

	def register(node: TypedExprNode, key: String, value: String) {
		table.getOrElseUpdate(node, Map[String, String]()) += (key -> value)
	}

	def register(node: TypedExprNode) {
		val entry = table.getOrElseUpdate(node, Map[String, String]())
		if(!entry.contains(EXPR_VARIABLE_NAME)) {
		  entry += (EXPR_VARIABLE_NAME -> name(node, "__expr_"))
		}
		if(!entry.contains(EXPR_NULL_INDICATOR_NAME)) {
		  entry += (EXPR_NULL_INDICATOR_NAME -> name(node, "__indicator_"))
		}
	}
	
	def codeIsValid(expr: TypedExprNode): String = getExprCode(expr, CODE_IS_VALID)
	def codeValueRepl(expr: TypedExprNode): String = getExprCode(expr, CODE_VALUE_REPL)
	def codeInvalidate(expr: TypedExprNode): String = getExprCode(expr, CODE_INVALIDATE)
	def codeValidate(expr: TypedExprNode): String = getExprCode(expr, CODE_VALIDATE)
	def exprName(expr: TypedExprNode): String = getExprCode(expr, EXPR_VARIABLE_NAME)
	def exprType(expr: TypedExprNode): String = getExprCode(expr, EXPR_VARIABLE_TYPE)
	def indicatorName(expr: TypedExprNode): String = getExprCode(expr, EXPR_NULL_INDICATOR_NAME)
	def indicatorDefaultValue(expr: TypedExprNode): String = getExprCode(expr, EXPR_NULL_INDICATOR_DEFAULT_VALUE)
	def exprDefaultValue(expr: TypedExprNode, default: String): String = {
	  val defValue = getExprCode(expr, EXPR_DEFAULT_VALUE)
	  if(defValue == null) default else defValue
	}
}

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

//	def propertyC(defType: Class[_], isCreate: Boolean = true, isFinal: Boolean = false, initString: String = null, isStatic: Boolean = false): String = {
//	  property(defType.getCanonicalName(), isCreate, isFinal, initString, isStatic)
//	}

	def property(defType: String, isCreate: Boolean = true, isFinal: Boolean = false, initString: String = null, isStatic: Boolean = false): String = {
		val pd = new PropertyDefinition(defType, isCreate, isFinal, isStatic, initString)
		properties += (pd)

		pd.defName
	}

}

case class EENAttribute(expr: TENAttribute, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	override def exprCode(ctx: CGExprContext) = """%s.%s""".format(ctx.exprName(expr.child), expr.escapedName)
}

case class EENLiteral(expr: TENLiteral, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	override def initial(ctx: CGExprContext) {
	  TypeUtil.assertDataType(expr.outputDT)
		val variableName = expr.obj match {
			case null => if(expr.writable) {
			  ctx.property(expr.dt.writable, false, true, null, true)
			} else {
			  ctx.property(expr.dt.primitive, false, true, null, true)
			}
			case x: NullWritable => "null"
			case x: Text => {
			  val t = if(expr.writable) classOf[Text].getCanonicalName() else "String"
			  val v: String = if(expr.writable) string2Text(x) else x
			  
			  ctx.property(t, true, true, v, true)
			}
			case x: String => {
			  val t = if(expr.writable) classOf[Text].getCanonicalName() else "String"
			  val v: String = if(expr.writable) { 
			    string2Text(textConvert2ByteArrayInHex(x)) 
			  } else {
			    textConvert2ByteArrayInHex(x)
			  }
			  ctx.property(t, true, true, v, true)
			}
			case x: BytesWritable => {
			  val t = if(expr.writable) classOf[BytesWritable].getCanonicalName() else "byte[]"
			  val v: String = if(expr.writable) bytes2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: IntWritable => {
			  val t = if(expr.writable) classOf[IntWritable].getCanonicalName() else "int"
			  val v: String = if(expr.writable) int2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Integer => {
			  val t = if(expr.writable) classOf[IntWritable].getCanonicalName() else "int"
			  val v: String = if(expr.writable) int2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: BooleanWritable => {
			  val t = if(expr.writable) classOf[BooleanWritable].getCanonicalName() else "boolean"
			  val v: String = if(expr.writable) boolean2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Boolean => {
			  val t = if(expr.writable) classOf[BooleanWritable].getCanonicalName() else "boolean"
			  val v: String = if(expr.writable) boolean2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: FloatWritable => {
			  val t = if(expr.writable) classOf[FloatWritable].getCanonicalName() else "float"
			  val v: String = if(expr.writable) float2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Float => {
			  val t = if(expr.writable) classOf[FloatWritable].getCanonicalName() else "float"
			  val v: String = if(expr.writable) float2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: DoubleWritable => {
			  val t = if(expr.writable) classOf[DoubleWritable].getCanonicalName() else "double"
			  val v: String = if(expr.writable) double2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Double => {
			  val t = if(expr.writable) classOf[DoubleWritable].getCanonicalName() else "double"
			  val v: String = if(expr.writable) double2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: LongWritable => {
			  val t = if(expr.writable) classOf[LongWritable].getCanonicalName() else "long"
			  val v: String = if(expr.writable) long2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Long => {
			  val t = if(expr.writable) classOf[LongWritable].getCanonicalName() else "long"
			  val v: String = if(expr.writable) long2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: ByteWritable => {
			  val t = if(expr.writable) classOf[ByteWritable].getCanonicalName() else "byte"
			  val v: String = if(expr.writable) byte2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Byte => {
			  val t = if(expr.writable) classOf[ByteWritable].getCanonicalName() else "byte"
			  val v: String = if(expr.writable) byte2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: ShortWritable => {
			  val t = if(expr.writable) classOf[ShortWritable].getCanonicalName() else "short"
			  val v: String = if(expr.writable) short2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.lang.Short => {
			  val t = if(expr.writable) classOf[ShortWritable].getCanonicalName() else "short"
			  val v: String = if(expr.writable) short2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: TimestampWritable => {
			  val t = if(expr.writable) {
			    classOf[TimestampWritable].getCanonicalName() 
			  } else {
			    classOf[java.sql.Timestamp].getCanonicalName()
			  }
			  val v: String = if(expr.writable) timestamp2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case x: java.sql.Timestamp => {
			  val t = if(expr.writable) {
			    classOf[TimestampWritable].getCanonicalName() 
			  } else {
			    classOf[java.sql.Timestamp].getCanonicalName()
			  }
			  val v: String = if(expr.writable) timestamp2Writable(x) else x
			  ctx.property(t, true, true, v, true)
			}
			case _ => throw new CGAssertRuntimeException("TODO")
		}

		if(expr.writable) ctx.defineImport(expr.outputDT.writable)
		
		ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, null)
		ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
		ctx.register(expr, ctx.EXPR_VARIABLE_NAME, variableName)
		ctx.register(expr, ctx.CODE_VALUE_REPL, variableName)
		ctx.register(expr, ctx.CODE_VALIDATE, "")
		ctx.register(expr, ctx.CODE_IS_VALID, null)
	}

	override def exprCode(ctx: CGExprContext) = null
}

case class EENUDF(expr: TENUDF, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	private var udf: String = _

	override def initial(ctx: CGExprContext) {
	  TypeUtil.assertDataType(expr.outputDT)
		udf = ctx.property(expr.bridge.getUdfClass().getCanonicalName())

		ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
		ctx.register(expr, ctx.CODE_IS_VALID, "%s != null".format(ctx.exprName(expr)))
		ctx.register(expr, ctx.CODE_VALIDATE, "")
		ctx.register(expr, ctx.CODE_INVALIDATE, null)
		ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
	}

	override def exprCode(ctx: CGExprContext) = {
		"%s.evaluate(%s)".format(udf, 
          if(expr.children.length == 0) 
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
		ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
		ctx.register(expr, ctx.CODE_IS_VALID, "%s != null".format(ctx.exprName(expr)))
		ctx.register(expr, ctx.CODE_VALIDATE, "")
		ctx.register(expr, ctx.CODE_INVALIDATE, null)
		ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
	}

	override def exprCode(ctx: CGExprContext) = {
		// TODO should use the lazy computing for performance purpose, trigger the child node 
		// evaluating when it's called (properly we could do that in a coded DeferredObject)
		"(%s)%s.evaluate(new DeferredObject[]{%s})".format(
				expr.outputDT.writable,
				gudf, 
				if(expr.children.length == 0) 
				  "" 
			    else 
				  expr.children.map(ctx.exprName(_)).reduce((a, b) => a + "," + b))
	}
}

case class EENBuiltin(expr: TENBuiltin, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	override def exprCode(ctx: CGExprContext): String = if (expr.exprs.length == 1) {
		expr.op match {
			case "isnull" => "!(%s)".format(ctx.codeIsValid(expr.children(0)))
			case "isnotnull" => "%s".format(ctx.codeIsValid(expr.children(0)))
			case "istrue" => "%s".format(ctx.codeValueRepl(expr.children(0)))
			case "isfalse" => "!(%s)".format(ctx.codeValueRepl(expr.children(0)))
			case _ => "%s%s".format(expr.op, ctx.codeValueRepl(expr.children(0)))
		}
	} else {
	  expr.op match {
	    case ">" => expr.children(0).outputDT match {
	      case TypeUtil.StringType => "%s.compareTo(%s) > 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.BinaryType => {
	        "org.apache.hadoop.io.WritableComparator.compareBytes(%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) > 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      }
	      case TypeUtil.TimestampType => "%s.compareTo(%s) > 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case _ => defaultExpr(ctx)
	    }
		case ">=" => expr.children(0).outputDT match {
	      case TypeUtil.StringType => "%s.compareTo(%s) >= 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.BinaryType =>"org.apache.hadoop.io.WritableComparator.compareBytes(%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) >= 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.TimestampType =>"%s.compareTo(%s) >= 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case _ => defaultExpr(ctx)
	    }
		case "=" => expr.children(0).outputDT match {
	      case TypeUtil.StringType =>"%s.equals(%s)".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.BinaryType =>"org.apache.hadoop.io.WritableComparator.compareBytes(%1$s, 0, %1$s.length, %2$s, 0, %2$s.length).compareTo(%s) == 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.TimestampType =>"%s.equals(%s)".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case _ => defaultExpr(ctx)
	    }
		case "!=" => expr.children(0).outputDT match {
	      case TypeUtil.StringType =>"!%s.equals(%s)".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.BinaryType =>"org.apache.hadoop.io.WritableComparator.compareBytes(%1$s, 0, %1$s.length, %2$s, 0, %2$s.length).compareTo(%s) != 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.TimestampType =>"!%s.equals(%s)".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case _ => defaultExpr(ctx)
	    }
		case "<=" => expr.children(0).outputDT match {
	      case TypeUtil.StringType =>"%s.compareTo(%s) <= 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.BinaryType =>"org.apache.hadoop.io.WritableComparator.compareBytes(%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) <= 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.TimestampType =>"%s.compareTo(%s) <= 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case _ => defaultExpr(ctx)
	    }
		case "<" => expr.children(0).outputDT match {
	      case TypeUtil.StringType =>"%s.compareTo(%s) < 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.BinaryType =>"org.apache.hadoop.io.WritableComparator.compareBytes(%1$s, 0, %1$s.length, %2$s, 0, %2$s.length) < 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case TypeUtil.TimestampType =>"%s.compareTo(%s) < 0".format(ctx.exprName(expr.children(0)), ctx.exprName(expr.children(1)))
	      case _ => defaultExpr(ctx)
	    }
		case _ => defaultExpr(ctx)
	  }
	}
	
	private def defaultExpr(ctx: CGExprContext): String = {
	  val snippet = expr.exprs.map(ctx.exprName(_)).reduce((a, b) => { "%s%s%s".format(a, expr.op, b) })
	  expr.outputDT match {
	    case TypeUtil.ByteType => "(byte)(%s)".format(snippet)
	    case TypeUtil.ShortType => "(short)(%s)".format(snippet)
	    case _ => snippet
	  }
	}
}

case class EENConvertR2R(expr: TENConvertR2R, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
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
			case TypeUtil.BooleanType => "(%s ? new java.util.Date(1l) : new java.util.Date(0l)".format(fromExprName)
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
			case TypeUtil.BooleanType => "(%s ? new java.sql.Timestamp(1l) : new java.sql.Timestamp(0l)".format(fromExprName)
			case TypeUtil.FloatType => "new java.sql.Timestamp(%s)".format(fromExprName)
			case TypeUtil.DoubleType => "new java.sql.Timestamp(%s)".format(fromExprName)
			case TypeUtil.LongType => "new java.sql.Timestamp(%s)".format(fromExprName)
			case TypeUtil.ByteType => "new java.sql.Timestamp(%s)".format(fromExprName)
			case TypeUtil.ShortType => "new java.sql.Timestamp(%s)".format(fromExprName)
			case _ => throw new CGAssertRuntimeException(dtFrom + "can not convert to Timestamp")
		}
	}
	
	override def initial(ctx: CGExprContext) {
	  if(expr.from.isInstanceOf[TENLiteral]) {
	    val from = expr.from.asInstanceOf[TENLiteral]
	    if(from.obj != null) {
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
	  ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
	  ctx.register(expr, ctx.CODE_IS_VALID, null)
	  ctx.register(expr, ctx.CODE_INVALIDATE, null)
	  ctx.register(expr, ctx.CODE_VALIDATE, null)
	}
}

case class EENConvertR2W(expr: TENConvertR2W, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	var storageName: String = _

	override def initial(ctx: CGExprContext) {
		storageName = ctx.property(expr.outputDT.writable)
		
		ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
		ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
		ctx.register(expr, ctx.CODE_IS_VALID, "%s!=null".format(ctx.exprName(expr)))
		ctx.register(expr, ctx.CODE_INVALIDATE, "%s = null".format(ctx.exprName(expr)))
		ctx.register(expr, ctx.CODE_VALIDATE, "")
		ctx.register(expr, ctx.CODE_VALUE_REPL, ctx.exprName(expr))
	}

	override def exprCode(ctx: CGExprContext) = "%s.build(%s, %s)".format(TypeUtil.getSetWritableClass().getCanonicalName(), storageName, ctx.exprName(expr.from))
}

case class EENConvertW2R(expr: TENConvertW2R, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	override def initial(ctx: CGExprContext) {
		ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, expr.from.outputDT.primitive)
	}
	override def exprCode(ctx: CGExprContext) = expr.from.outputDT match {
		case TypeUtil.StringType => "%s.toString()".format(ctx.exprName(expr.from))
		case TypeUtil.BinaryType => "%s.getBytes(%s)".format(TypeUtil.getSetRawClass().getCanonicalName(), ctx.exprName(expr.from))
		case TypeUtil.TimestampType => "%s.getTimestamp()".format(ctx.exprName(expr.from))
		
		case _ => "%s.get()".format(ctx.exprName(expr.from))
	}
}

case class EENConvertW2D(expr: TENConvertW2D, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	var deferredName: String = _

	override def initial(ctx: CGExprContext) {
		ctx.defineImport(TypeUtil.getDeferredObjectClass())
		deferredName = ctx.property(TypeUtil.getDeferredObjectClass().getCanonicalName())
		
		ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, TypeUtil.getDeferredObjectClass().getCanonicalName())
		ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
		ctx.register(expr, ctx.CODE_IS_VALID, null)
		ctx.register(expr, ctx.CODE_VALIDATE, "")
	}
	
	override def exprCode(ctx: CGExprContext) = "%s.build(%s)".format(deferredName, ctx.exprName(expr.from))
}

