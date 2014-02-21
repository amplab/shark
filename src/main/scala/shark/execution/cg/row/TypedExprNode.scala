package shark.execution.cg.row

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf._
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }

import org.apache.spark.Logging

import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.CGUtil

abstract class TypedExprNode extends ExprNode[TypedExprNode] {
	self: Product =>

	// Current node is deterministic, if current UDF and all children nodes are deterministic 
	val isDeterministic = true

	// Current node is stateful if current UDF or one of the child node is stateful
	val isStateful = false

	// output data type
	var outputDT: DataType = _

	val possibleNullValue = true

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
}

object TypedExprNode {
	val incr = new AtomicInteger()
	def randomName(prefix: String) = prefix + incr.getAndIncrement()
}

case class TENLiteral(obj: Any = null, dt: DataType = null) extends TypedExprNode with LeafNode[TypedExprNode] {
	self: Product =>

	outputDT = dt
	override val possibleNullValue = (obj == null)
}

case class TENInputRow(struct: CGStruct, attr: String = null) extends TypedExprNode with LeafNode[TypedExprNode] {
	self: Product =>
	outputDT = struct
	override val possibleNullValue = false
	override val exprId = "__input__"

	lazy val field = struct.getField(attr)
	
	def escapedName = CGUtil.makeCGFieldName(attr)
	def maskBitName = CGField.getMaskBitVariableName(escapedName)
}

case class TENOutputField(attr: String, expr: TypedExprNode, dt: DataType) extends TypedExprNode with UnaryNode[TypedExprNode] {
	self: Product =>
	outputDT = dt

	def child = expr
	override val exprId = "__output__"
	  
	def escapedName = CGUtil.makeCGFieldName(attr)
	def maskBitName = CGField.getMaskBitVariableName(escapedName)
}

case class TENOutputExpr(expr: TypedExprNode) extends TypedExprNode with UnaryNode[TypedExprNode] {
	self: Product =>
	outputDT = expr.outputDT

	def child = expr
	override val exprId = "__filter__"
}

case class TENOutputRow(fields: Seq[TypedExprNode], output: CGStruct) extends TypedExprNode {
	self: Product =>
	outputDT = output

	def children = fields

	override val possibleNullValue = false
}

abstract class TENConvert(from: TypedExprNode, to: DataType) extends TypedExprNode with UnaryNode[TypedExprNode] {
	self: Product =>

	override def child = from
	outputDT = to

	override val isDeterministic = from.isDeterministic
}

/**
 * Raw type to Raw type
 */
case class TENConvertR2R(from: TypedExprNode, to: DataType) extends TENConvert(from, to)

/**
 * Raw type to Writable
 */
case class TENConvertR2W(from: TypedExprNode) extends TENConvert(from, from.outputDT)

/**
 * Raw type to DeferredObject
 */
case class TENConvertW2D(from: TypedExprNode) extends TENConvert(from, from.outputDT)

/**
 * Writable to Raw 
 */
case class TENConvertW2R(from: TypedExprNode) extends TENConvert(from, from.outputDT)

// TODO need to consider about the union
case class TENAttribute(attr: String, expr: TypedExprNode) extends TypedExprNode with UnaryNode[TypedExprNode] {
	self: Product =>
	outputDT = expr.outputDT.asInstanceOf[CGStruct].getField(attr)
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
	outputDT = {
		import org.apache.hadoop.util.ReflectionUtils

		// TODO need to think about how to convert the Struct / map / union / list to DataType
		val output = bridge.initializeAndFoldConstants(exprs.map(_.outputDT.oi.asInstanceOf[OI]).toArray)

		TypeUtil.getDataType(output)
	}

	private lazy val udf = bridge.getUdfClass().newInstance()

	def children = exprs

	override val isDeterministic = exprs.foldLeft(
		FunctionRegistry.isDeterministic(bridge))(_ && _.isDeterministic)

	override val isStateful = FunctionRegistry.isStateful(bridge)
}

case class TENGUDF(clazz: Class[_ <: GenericUDF], exprs: Seq[TypedExprNode]) extends TypedExprNode {
	self: Product =>
	outputDT = {
		import org.apache.hadoop.util.ReflectionUtils

		val genericUDF = ReflectionUtils.newInstance(clazz, null).asInstanceOf[GenericUDF]
		// TODO need to think about how to convert the Struct / map / union / list to DataType
		val output = genericUDF.initializeAndFoldConstants(exprs.map(_.outputDT.oi.asInstanceOf[OI]).toArray)

		TypeUtil.getDataType(output)
	}

	private lazy val gudf = clazz.newInstance()

	def children = exprs

	override val isDeterministic = exprs.foldLeft(
		FunctionRegistry.isDeterministic(gudf))(_ && _.isDeterministic)

	override val isStateful = FunctionRegistry.isStateful(gudf)
}

// TODO replace the nullCheckRequired with type Seq[Boolean] for indicating each of the child node if it's the require null check
// just in case of the rewrite the GenericUDF like printf()
case class TENBuiltin(op: String, exprs: Seq[TypedExprNode], dt: DataType, nullCheckRequired: Boolean = true) extends TypedExprNode {
	self: Product =>
	outputDT = dt
	override val isDeterministic = children.foldLeft(true)((a, b) => { a && b.isDeterministic })
	
	def children = exprs
}

case class TENBranch(branchIf: TypedExprNode, branchThen: TypedExprNode, branchElse: TypedExprNode) extends TypedExprNode {
	self: Product =>
	outputDT = branchThen.outputDT

	override def children = (branchIf :: branchThen :: branchElse :: Nil).filter(_ != null)

	override val isDeterministic = children.foldLeft(true)((a, b) => { a && b.isDeterministic })
}
