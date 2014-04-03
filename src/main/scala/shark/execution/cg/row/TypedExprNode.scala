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

import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf._
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.objectinspector.{ PrimitiveObjectInspector => POI }
import org.apache.hadoop.hive.serde2.objectinspector.{ ConstantObjectInspector }
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
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

import org.apache.spark.Logging

import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.CGInvalidException
import shark.execution.cg.CGUtil

/**
 * Expr tree node of representing the expression 
 */
abstract class TypedExprNode extends ExprNode[TypedExprNode] {
  self: Product =>

  // Current node is deterministic, if current UDF and all children nodes are deterministic 
  def isDeterministic = true

  // Current node is stateful if current UDF or one of the child node is stateful
  def isStateful = false

  // output data type
  def outputDT: DataType = null

  val exprId = TypedExprNode.randomName("__expr_")

  private lazy val hashcode: Int = children.foldLeft(this.getClass().hashCode())(_ + _.hashCode)
  override def hashCode = hashcode
  override def equals(ref: Any) = if (this eq ref.asInstanceOf[AnyRef]) {
    true
  } else if (ref == null || ref.getClass() != this.getClass()) {
    false
  } else {
    // if it's the non-deterministic node, then will fail in equality comparison, unless 
    // exactly the same instance
    isDeterministic && 
    (hashCode == ref.hashCode()) && ({
      val that = ref.asInstanceOf[this.type].productIterator.toList
      val thiz = productIterator.toList
      that == thiz
    })
  }
  
  // convert the current node to the one which output result is java object(primitive) 
  def toR: TypedExprNode = if(outputWritable) TENConvertW2R(this) else this
  
  // convert the current node to the one which output result is Writable object
  def toW: TypedExprNode = if(outputWritable) this else TENConvertR2W(this)
  
  def outputWritable = false
}

/**
 * Common Utilities
 */
object TypedExprNode {
  val incr = new AtomicInteger()
  def randomName(prefix: String) = prefix + incr.getAndIncrement()

  def deterministic(bridge: GenericUDFBridge, exprs: Seq[TypedExprNode]): Boolean = {
    exprs.foldLeft(FunctionRegistry.isDeterministic(bridge))(_ && _.isDeterministic)
  }
  
  def deterministic(bridge: GenericUDF, exprs: Seq[TypedExprNode]): Boolean = {
    exprs.foldLeft(FunctionRegistry.isDeterministic(bridge))(_ && _.isDeterministic)
  }
  
  def stateful(bridge: GenericUDFBridge): Boolean = {
    FunctionRegistry.isStateful(bridge)
  }
  
  def stateful(bridge: GenericUDF): Boolean = {
    FunctionRegistry.isStateful(bridge)
  }
  
  def initializeGUDF(genericUDF: GenericUDF, children: Seq[TypedExprNode]): OI = {
    // TODO need to think about how to convert the Struct / map / union / list to DataType
    genericUDF.initializeAndFoldConstants(TENFactory.r2w(children).map(node => {
      node match {
        case x: TENLiteral => x.constantOI
        case _ => node.outputDT.oi.asInstanceOf[OI]
      }
    }).toArray)
  }
}

/**
 * Constant Value(writable and primitive)
 */
case class TENLiteral(obj: Any, dt: DataType, writable: Boolean) extends TypedExprNode 
  with LeafNode[TypedExprNode] {
  
  self: Product =>
  override def outputDT = dt

  // TODO need to think about the array/map/list/struct type
  def constantOI: ConstantObjectInspector = {
    val pc = dt.oi.asInstanceOf[POI].getPrimitiveCategory()
    if (writable || obj == null) {
      POIF.getPrimitiveWritableConstantObjectInspector(pc, obj)
    } else {
      POIF.getPrimitiveWritableConstantObjectInspector(pc, pc match {
        case PrimitiveCategory.BOOLEAN => 
          POIF.writableBooleanObjectInspector.create(obj.asInstanceOf[Boolean])
        case PrimitiveCategory.BYTE => 
          POIF.writableByteObjectInspector.create(obj.asInstanceOf[Byte])
        case PrimitiveCategory.SHORT => 
          POIF.writableShortObjectInspector.create(obj.asInstanceOf[Short])
        case PrimitiveCategory.INT => 
          POIF.writableIntObjectInspector.create(obj.asInstanceOf[Int])
        case PrimitiveCategory.LONG => 
          POIF.writableLongObjectInspector.create(obj.asInstanceOf[Long])
        case PrimitiveCategory.FLOAT => 
          POIF.writableFloatObjectInspector.create(obj.asInstanceOf[Float])
        case PrimitiveCategory.DOUBLE => 
          POIF.writableDoubleObjectInspector.create(obj.asInstanceOf[Double])
        case PrimitiveCategory.STRING => 
          POIF.writableStringObjectInspector.create(obj.asInstanceOf[String])
        case PrimitiveCategory.TIMESTAMP => 
          POIF.writableTimestampObjectInspector.create(obj.asInstanceOf[java.sql.Timestamp])
        case PrimitiveCategory.BINARY => 
          POIF.writableBinaryObjectInspector.create(obj.asInstanceOf[Array[Byte]])
      })
    }
  }
  
  override def outputWritable = writable
  override def toR: TypedExprNode = if(writable) convert else this
  override def toW: TypedExprNode = if(writable) this else convert
  
  // TODO need to think about the array/map/list/struct type
  private def convert = if(obj == null) TENLiteral(null, dt, !writable) else {
    val pc = dt.oi.asInstanceOf[POI].getPrimitiveCategory()
    TENLiteral(if (writable) {
      pc match {
        case PrimitiveCategory.BOOLEAN => obj.asInstanceOf[BooleanWritable].get()
        case PrimitiveCategory.BYTE => obj.asInstanceOf[ByteWritable].get()
        case PrimitiveCategory.SHORT => obj.asInstanceOf[ShortWritable].get()
        case PrimitiveCategory.INT => obj.asInstanceOf[IntWritable].get()
        case PrimitiveCategory.LONG => obj.asInstanceOf[LongWritable].get()
        case PrimitiveCategory.FLOAT => obj.asInstanceOf[FloatWritable].get()
        case PrimitiveCategory.DOUBLE => obj.asInstanceOf[DoubleWritable].get()
        case PrimitiveCategory.STRING => obj.asInstanceOf[Text].toString
        case PrimitiveCategory.TIMESTAMP => obj.asInstanceOf[TimestampWritable].getTimestamp()
        case PrimitiveCategory.BINARY => obj.asInstanceOf[BytesWritable].get()
      }
    } else {
      pc match {
        case PrimitiveCategory.BOOLEAN => new BooleanWritable(obj.asInstanceOf[Boolean])
        case PrimitiveCategory.BYTE => new ByteWritable(obj.asInstanceOf[Byte])
        case PrimitiveCategory.SHORT => new ShortWritable(obj.asInstanceOf[Short])
        case PrimitiveCategory.INT => new IntWritable(obj.asInstanceOf[Int])
        case PrimitiveCategory.LONG => new LongWritable(obj.asInstanceOf[Long])
        case PrimitiveCategory.FLOAT => new FloatWritable(obj.asInstanceOf[Float])
        case PrimitiveCategory.DOUBLE => new DoubleWritable(obj.asInstanceOf[Double])
        case PrimitiveCategory.STRING => new Text(obj.asInstanceOf[String])
        case PrimitiveCategory.TIMESTAMP => 
          new TimestampWritable(obj.asInstanceOf[java.sql.Timestamp])
        case PrimitiveCategory.BINARY => new BytesWritable(obj.asInstanceOf[Array[Byte]])
      }
    }, dt, !writable)
  }
}

case class TENInputRow(struct: CGStruct, attr: String) extends TypedExprNode 
  with LeafNode[TypedExprNode] {
  self: Product =>
    
  override def outputDT = field

  lazy val field = struct.getField(attr)
  
  def escapedName = CGUtil.makeCGFieldName(attr)
  def maskBitName = CGField.getMaskBitVariableName(escapedName)
  
  override def outputWritable = TypeUtil.isWritable(field)
}

case class TENOutputField(attr: String, expr: TypedExprNode, dt: DataType) extends TypedExprNode 
  with UnaryNode[TypedExprNode] {
  self: Product =>
    
  override def outputDT = dt

  def child = expr
  override val exprId = "__output__"
    
  def escapedName = CGUtil.makeCGFieldName(attr)
  def maskBitName = CGField.getMaskBitVariableName(escapedName)
  
  override def outputWritable = false
}

case class TENOutputWritableField(fieldIdx: Int, expr: TypedExprNode, dt: DataType) 
  extends TypedExprNode with UnaryNode[TypedExprNode] {
  self: Product =>
    
  override def outputDT = dt

  def child = expr
  override val exprId = "__output__"
    
  override def outputWritable = true
}

case class TENOutputExpr(expr: TypedExprNode) extends TypedExprNode with UnaryNode[TypedExprNode] {
  self: Product =>
  override def outputDT = expr.outputDT

  def child = expr
  override val exprId = "__filter__"
  override def outputWritable = false
}

case class TENOutputRow(fields: Seq[TypedExprNode], output: CGStruct) extends TypedExprNode {
  self: Product =>
    
  override def outputDT = output

  def children = fields
}

abstract class TENConvert(from: TypedExprNode, to: DataType) extends TypedExprNode 
  with UnaryNode[TypedExprNode] {
  self: Product =>

  override def child = from
  override def outputDT = to

  override val isDeterministic = from.isDeterministic
}

/**
 * Raw type to Raw type
 */
case class TENConvertR2R(from: TypedExprNode, to: DataType) extends TENConvert(from, to) {
}

/**
 * Raw type to Writable
 */
case class TENConvertR2W(from: TypedExprNode) extends TENConvert(from, from.outputDT) {
  override def outputWritable = true
}

/**
 * Raw type to DeferredObject
 */
case class TENConvertW2D(from: TypedExprNode) extends TENConvert(from, from.outputDT) {
  override def toR: TypedExprNode = 
    throw new CGInvalidException("shouldn't be called TENConvertW2D")
  override def toW: TypedExprNode = 
    throw new CGInvalidException("shouldn't be called TENConvertW2D")
  override def outputWritable = 
    throw new CGInvalidException("shouldn't be called TENConvertW2D")
}

/**
 * Writable to Raw 
 */
case class TENConvertW2R(from: TypedExprNode) extends TENConvert(from, from.outputDT) {
}

/**
 * Attribute is currently not used, as the embeded struct are not support yet. 
 */
case class TENAttribute(attr: String, expr: TypedExprNode) extends TypedExprNode 
  with UnaryNode[TypedExprNode] {
  self: Product =>

  override def outputDT = expr.outputDT.asInstanceOf[CGStruct].getField(attr)
  override def child = expr
  
    def escapedName = CGUtil.makeCGFieldName(attr)
  def maskBitName = CGField.getMaskBitVariableName(escapedName)
}

// TODO will be used in the future
//case class TENAlias(expr: TypedExprNode) extends TypedExprNode with UnaryNode[TypedExprNode] {
//  self: Product =>
//  override def child = expr
//  outputDT = expr.outputDT
//  
//  override val isDeterministic = expr.isDeterministic
//
//  override val isStateful = expr.isStateful
//    
//  override def hashCode = expr.hashCode
//  override def equals(ref: Any) = super.equals(ref) || expr.equals(ref)
//  
//  override def simpleString = "alias: %s".format(expr.simpleString)
//}


case class TENUDF(bridge: GenericUDFBridge, exprs: Seq[TypedExprNode]) extends TypedExprNode {
  self: Product =>
  private lazy val outputOI = TypedExprNode.initializeGUDF(bridge, exprs)
  private lazy val dt = TypeUtil.getDataType(outputOI)
  
  override def outputDT = dt

  private lazy val udf = bridge.getUdfClass().newInstance()

  def children = exprs

  override val isDeterministic = TypedExprNode.deterministic(bridge, exprs)

  override val isStateful = TypedExprNode.stateful(bridge)
  
  override def equals(ref: Any) = if (this eq ref.asInstanceOf[AnyRef]) {
    true
  } else if (ref == null || ref.getClass() != this.getClass()) {
    false
  } else {
    // if it's the non-deterministic node, then will fail in equality comparison, unless 
    // exactly the same instance
    if(isDeterministic && hashCode == ref.hashCode()) {
      val that = ref.asInstanceOf[TENUDF]
      val thisUDFClassName = bridge.getUdfClass().getCanonicalName()
      val thatUDFClassName = that.bridge.getUdfClass().getCanonicalName()
      
      thisUDFClassName.equals(thatUDFClassName) && (exprs == that.exprs)
    } else {
      false
    } 
  }
  
  override def outputWritable = TypeUtil.isWritable(outputOI)
}

case class TENGUDF(genericUDF: GenericUDF, exprs: Seq[TypedExprNode]) extends TypedExprNode {
  self: Product =>
  private lazy val outputOI = TypedExprNode.initializeGUDF(genericUDF, exprs)
  private lazy val dt = TypeUtil.getDataType(outputOI)
  
  override def outputDT = dt

  def children = exprs

  override val isDeterministic = TypedExprNode.deterministic(genericUDF, exprs)

  override val isStateful = TypedExprNode.stateful(genericUDF)
  
  override def equals(ref: Any) = if (this eq ref.asInstanceOf[AnyRef]) {
    true
  } else if (ref == null || ref.getClass() != this.getClass()) {
    false
  } else {
    // if it's the non-deterministic node, then will fail in equality comparison, unless 
    // exactly the same instance
    if(isDeterministic && hashCode == ref.hashCode()) {
      val that = ref.asInstanceOf[TENGUDF]
      genericUDF.getClass().equals(that.genericUDF.getClass()) && {
        exprs == that.exprs
      }
    } else {
      false
    } 
  }  
  
  override def outputWritable = TypeUtil.isWritable(outputOI)
}

/**
 * Represent the expressions(UDF / generic UDF) that rewritten, will generate the code in java
 * e.g. UDFAdd ==> java code "x + y"
 * e.g. GenericUDFBetween => java code "if(x >= a && y <= b) true else false" 
 * 
 * Some case may not require the null check in the generated java code, but some does.
 * a. without null checking
 * e.g. isnull a => java code: "a==null"
 * b. with null checking
 * e.g. x + y => java code: "if(x != null) if(y != null) x + y else null else null"
 */
case class TENBuiltin(op: TENBuiltin.OP, exprs: Seq[TypedExprNode], dt: DataType, 
  nullCheckRequired: Boolean = true) extends TypedExprNode {
  
  self: Product =>

  override def outputDT = dt
  override val isDeterministic = children.foldLeft(true)((a, b) => { a && b.isDeterministic })
  
  def children = exprs
}

object TENBuiltin {
  case class OP(symbol: String, resultType: Int) {
    def parameterDataType(paramType: DataType): DataType = {
      if(resultType == OP.RT_NUMERIC) {
        if(paramType == TypeUtil.IntegerType ||
          paramType == TypeUtil.BooleanType ||
          paramType == TypeUtil.FloatType ||
          paramType == TypeUtil.DoubleType ||
          paramType == TypeUtil.LongType ||
          paramType == TypeUtil.ByteType ||
          paramType == TypeUtil.ShortType) {
          paramType
        } else {
          TypeUtil.DoubleType
        }
      } else {
        paramType
      }
    }
    
    def resultDataType(paramType: DataType): DataType = {
      if(resultType == OP.RT_NUMERIC) {
        paramType
      } else {
        TypeUtil.BooleanType
      }
    }
  }
  
  object OP {
    val RT_NUMERIC = 0
    val RT_LOGICAL = 1
  }

  val OP_PLUS = OP("+", OP.RT_NUMERIC)
  val OP_MINUS = OP("-", OP.RT_NUMERIC)
  val OP_MULTIPLY = OP("*", OP.RT_NUMERIC)
  val OP_DIVIDE = OP("/", OP.RT_NUMERIC)
  val OP_MOD = OP("%", OP.RT_NUMERIC)
  val OP_BIT_AND = OP("&", OP.RT_NUMERIC)
  val OP_BIT_OR = OP("|", OP.RT_NUMERIC)
  val OP_BIT_XOR = OP("^", OP.RT_NUMERIC)
  val OP_BIT_NOT = OP("~", OP.RT_NUMERIC)
  val OP_COND_ISNULL = OP("isnull", OP.RT_LOGICAL)
  val OP_COND_ISNOTNULL = OP("isnotnull", OP.RT_LOGICAL)
  val OP_COND_ISTRUE = OP("istrue", OP.RT_LOGICAL)
  val OP_COND_ISFALSE = OP("isfalse", OP.RT_LOGICAL)
  val OP_CMP_EQUAL = OP("==", OP.RT_LOGICAL)
  val OP_CMP_GREATE = OP(">", OP.RT_LOGICAL)
  val OP_CMP_GREATE_OR_EQUAL = OP(">=", OP.RT_LOGICAL)
  val OP_CMP_LESS = OP("<", OP.RT_LOGICAL)
  val OP_CMP_LESS_OR_EQUAL = OP("<=", OP.RT_LOGICAL)
  val OP_CMP_NOT_EQUAL = OP("!=", OP.RT_LOGICAL)
}

/**
 * Represent the condition in Java
 */
case class TENBranch(branchIf: TypedExprNode, branchThen: TypedExprNode, branchElse: TypedExprNode)
  extends TypedExprNode {
  self: Product =>
    
  override def outputDT = branchThen.outputDT

  override def children = (branchIf :: branchThen :: branchElse :: Nil).filter(_ != null)

  override val isDeterministic = children.foldLeft(true)((a, b) => { a && b.isDeterministic })
}
