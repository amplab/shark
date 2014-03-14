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
import shark.execution.cg.CGUtil

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
	
	def toR: TypedExprNode = if(outputWritable) TENConvertW2R(this) else this
	def toW: TypedExprNode = if(outputWritable) this else TENConvertR2W(this)
	
	def outputWritable = false
}

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

case class TENLiteral(obj: Any, dt: DataType, writable: Boolean) extends TypedExprNode with LeafNode[TypedExprNode] {
	self: Product =>
  override def outputDT = dt

  // TODO need to think about the array/map/list/struct type
  def constantOI: ConstantObjectInspector = {
    val pc = dt.oi.asInstanceOf[POI].getPrimitiveCategory()
    if (writable || obj == null) {
      POIF.getPrimitiveWritableConstantObjectInspector(pc, obj)
    } else {
      POIF.getPrimitiveWritableConstantObjectInspector(pc, pc match {
        case PrimitiveCategory.BOOLEAN => POIF.writableBooleanObjectInspector.create(obj.asInstanceOf[Boolean])
        case PrimitiveCategory.BYTE => POIF.writableByteObjectInspector.create(obj.asInstanceOf[Byte])
        case PrimitiveCategory.SHORT => POIF.writableShortObjectInspector.create(obj.asInstanceOf[Short])
        case PrimitiveCategory.INT => POIF.writableIntObjectInspector.create(obj.asInstanceOf[Int])
        case PrimitiveCategory.LONG => POIF.writableLongObjectInspector.create(obj.asInstanceOf[Long])
        case PrimitiveCategory.FLOAT => POIF.writableFloatObjectInspector.create(obj.asInstanceOf[Float])
        case PrimitiveCategory.DOUBLE => POIF.writableDoubleObjectInspector.create(obj.asInstanceOf[Double])
        case PrimitiveCategory.STRING => POIF.writableStringObjectInspector.create(obj.asInstanceOf[String])
        case PrimitiveCategory.TIMESTAMP => POIF.writableTimestampObjectInspector.create(obj.asInstanceOf[java.sql.Timestamp])
        case PrimitiveCategory.BINARY => POIF.writableBinaryObjectInspector.create(obj.asInstanceOf[Array[Byte]])
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
        case PrimitiveCategory.TIMESTAMP => new TimestampWritable(obj.asInstanceOf[java.sql.Timestamp])
        case PrimitiveCategory.BINARY => new BytesWritable(obj.asInstanceOf[Array[Byte]])
      }
    }, dt, !writable)
  }
}

case class TENInputRow(struct: CGStruct, attr: String) extends TypedExprNode with LeafNode[TypedExprNode] {
	self: Product =>
	override def outputDT = field

	lazy val field = struct.getField(attr)
	
	def escapedName = CGUtil.makeCGFieldName(attr)
	def maskBitName = CGField.getMaskBitVariableName(escapedName)
	
	override def outputWritable = TypeUtil.isWritable(field)
}

case class TENOutputField(attr: String, expr: TypedExprNode, dt: DataType) extends TypedExprNode with UnaryNode[TypedExprNode] {
	self: Product =>
	override def outputDT = dt

	def child = expr
	override val exprId = "__output__"
	  
	def escapedName = CGUtil.makeCGFieldName(attr)
	def maskBitName = CGField.getMaskBitVariableName(escapedName)
	
	override def outputWritable = false
}

case class TENOutputWritableField(fieldIdx: Int, expr: TypedExprNode, dt: DataType) extends TypedExprNode with UnaryNode[TypedExprNode] {
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

abstract class TENConvert(from: TypedExprNode, to: DataType) extends TypedExprNode with UnaryNode[TypedExprNode] {
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
  override def toR: TypedExprNode = throw new CGAssertRuntimeException("shouldn't be called TENConvertW2D")
  override def toW: TypedExprNode = throw new CGAssertRuntimeException("shouldn't be called TENConvertW2D")
  override def outputWritable = throw new CGAssertRuntimeException("shouldn't be called TENConvertW2D")
}

/**
 * Writable to Raw 
 */
case class TENConvertW2R(from: TypedExprNode) extends TENConvert(from, from.outputDT) {
}

// TODO need to consider about the union
case class TENAttribute(attr: String, expr: TypedExprNode) extends TypedExprNode with UnaryNode[TypedExprNode] {
	self: Product =>
	override def outputDT = expr.outputDT.asInstanceOf[CGStruct].getField(attr)
	override def child = expr
	
    def escapedName = CGUtil.makeCGFieldName(attr)
	def maskBitName = CGField.getMaskBitVariableName(escapedName)
}

//case class TENAlias(expr: TypedExprNode) extends TypedExprNode with UnaryNode[TypedExprNode] {
//	self: Product =>
//	override def child = expr
//	outputDT = expr.outputDT
//	
//	override val isDeterministic = expr.isDeterministic
//
//	override val isStateful = expr.isStateful
//		
//	override def hashCode = expr.hashCode
//	override def equals(ref: Any) = super.equals(ref) || expr.equals(ref)
//	
//	override def simpleString = "alias: %s".format(expr.simpleString)
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
		  bridge.getUdfClass().getCanonicalName().equals(that.bridge.getUdfClass().getCanonicalName()) && {
		    exprs == that.exprs
		  }
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

// TODO replace the nullCheckRequired with type Seq[Boolean] for indicating each of the child node if it's the require null check
// just in case of the rewrite the GenericUDF like printf()
case class TENBuiltin(op: String, exprs: Seq[TypedExprNode], dt: DataType, nullCheckRequired: Boolean = true) extends TypedExprNode {
	self: Product =>
	override def outputDT = dt
	override val isDeterministic = children.foldLeft(true)((a, b) => { a && b.isDeterministic })
	
	def children = exprs
}

case class TENBranch(branchIf: TypedExprNode, branchThen: TypedExprNode, branchElse: TypedExprNode) extends TypedExprNode {
	self: Product =>
	override def outputDT = branchThen.outputDT

	override def children = (branchIf :: branchThen :: branchElse :: Nil).filter(_ != null)

	override val isDeterministic = children.foldLeft(true)((a, b) => { a && b.isDeterministic })
}

