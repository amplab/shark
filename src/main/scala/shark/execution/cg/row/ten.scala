package shark.execution.cg.row

import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory

import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf._
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector

import org.apache.hadoop.hive.ql.exec.FunctionRegistry
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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import shark.execution.cg.CGAssertRuntimeException

class TENFactory {
	def create(name: String, desc: ExprNodeDesc, outputDT: DataType, input: TENInputRow): TENOutputField = {
		val expr = create(desc, input, false)
		// TODO need to think about how to identify the CGStruct / CGUnion etc.
		if (outputDT == null || outputDT == expr.outputDT) {
			TENOutputField(name, expr, expr.outputDT)
		} else {
			TENOutputField(name, TENConvertR2R(expr, outputDT), outputDT)
		}
	}
	
    def create(desc: ExprNodeDesc, expectedDT: DataType, input: TENInputRow): TENOutputExpr = {
		val expr = create(desc, input, false)
		if(expr.outputDT == expectedDT) {
		  TENOutputExpr(expr)
		} else {
		  TENOutputExpr(TENConvertR2R(expr, expectedDT))
		}
	}

	private def create(node: ExprNodeGenericFuncDesc, input: TENInputRow): TypedExprNode = {
		node.getGenericUDF() match {
			case x: GenericUDFOPAnd => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.and(children(0), children(1)) // AND (&&)
			}
			case x: GenericUDFOPOr => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.or(children(0), children(1)) // OR  (||)
			}
			case x: GenericUDFOPEqual => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("==", children, TypeUtil.BooleanType) // =
			}
			case x: GenericUDFOPNotEqual => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("!=", children, TypeUtil.BooleanType) // <> (!=)
			}
			case x: GenericUDFOPLessThan => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("<", children, TypeUtil.BooleanType) // <
			}
			case x: GenericUDFOPEqualOrLessThan => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("<=", children, TypeUtil.BooleanType) // <=
			}
			case x: GenericUDFOPGreaterThan => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin(">", children, TypeUtil.BooleanType) // >
			}
			case x: GenericUDFOPEqualOrGreaterThan => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin(">=", children, TypeUtil.BooleanType) // >=
			}
			case x: GenericUDFBetween => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.between(children) // between 
			}
			case x: GenericUDFOPNull => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.isnull(children(0)) // between // IS NULL
			}
			case x: GenericUDFOPNotNull => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.isnotnull(children(0)) // IS NOT NULL
			}
			case x: GenericUDFCase => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.branch_case(children)
			}
			case x: GenericUDFWhen => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.branch_when(children)
			}
			case x: GenericUDFIf => {
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.branch_if(children)
			}
			case x: GenericUDFBridge => {
				create(node, x, input) // bridge
			}
			case x: GenericUDF => {
				val children = node.getChildren().map(create(_, input, true)).toArray.toSeq
				TENFactory.gudf(x.getClass(), children) // call to the Generic UDF function
			}
		}
	}

	private def create(node: ExprNodeGenericFuncDesc, x: GenericUDFBridge, input: TENInputRow): TypedExprNode = {
		val udf = x.getUdfClass().newInstance()
		udf match {
			case x: UDFOPPlus => {// +
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("+", children) 
			}
			case x: UDFOPMinus => {// -
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("-", children) 
			}
			case x: UDFOPMultiply => {// *
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("*", children) 
			}
			case x: UDFOPDivide => {// /
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("/", children) 
			}
			case x: UDFOPLongDivide => {// / to the long integer
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("/", children) 
			}
			case x: UDFOPMod => {// %
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("%", children) 
			}
			case x: UDFPosMod => {// pmod ((a % b) + b) % b
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("%", 
					Seq(TENFactory.builtin("+", 
						Seq(TENFactory.builtin("%", children), children(1))), children(1))) 
			}
			case x: UDFOPBitAnd => {// &
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("&", children) 
			}
			case x: UDFOPBitOr => {// |
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("|", children) 
			}
			case x: UDFOPBitXor => {// ^
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("^", children) 
			}
			case x: UDFOPBitNot => {// ~
				val children = node.getChildren().map(create(_, input, false)).toArray.toSeq
				TENFactory.builtin("~", children) 
			}
			// un-recognized UDF will resort to the TENUDF, which generates code for UDF invoking 
			case _ => {
				val children = node.getChildren().map(create(_, input, true)).toArray.toSeq
				TENFactory.udf(x, children) // still reusing the GenericUDF
			}
		}
	}

	/**
	 * create the ExprNode , via the ExprNodeDesc
	 * In order to save time for the common sub expression nodes evaluating, identical ExprNodeDesc
	 * will get the same ExprNode instance. but there is an exception, if it's the
	 * ExprNodeGenericFuncDesc and deterministic-less
	 * (e.g. UDF rand() may always returns different value for each invokings)
	 */
	private def create(desc: ExprNodeDesc, input: TENInputRow, requireWritable: Boolean): TypedExprNode = {
		desc match {
			case x: ExprNodeGenericFuncDesc => {
				val f = create(x, input)
				if(requireWritable) {
					f match {
						case x: TENUDF => x
						case x: TENGUDF => x
						case _ => TENConvertR2W(f)
					}
				} else {
					f match {
						case x: TENUDF => TENConvertW2R(x)
						case x: TENGUDF => TENConvertW2R(x)
						case _ => f
					}
				}
			}
			case x: ExprNodeFieldDesc => TENFactory.attribute(x.getFieldName(), create(x.getDesc(), input, false))
			case x: ExprNodeColumnDesc => TENFactory.attribute(x.getColumn(), new TENInputRow(input.struct, x.getColumn())) // TODO should iterate every sub node
			case x: ExprNodeConstantDesc => TENFactory.literal(x.getValue(), TypeUtil.getDataType(x.getWritableObjectInspector()))
			case x: ExprNodeNullDesc => TENFactory.literal(null, null)
		}
	}
}

object constantTrue extends TENLiteral(true, TypeUtil.BooleanType)
object constantFalse extends TENLiteral(false, TypeUtil.BooleanType)
object constantNull extends TENLiteral(null, TypeUtil.NullType)

object TENFactory {
	import java.lang.reflect.Type
	import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
	import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
	import org.apache.hadoop.util.ReflectionUtils
	import scala.collection.JavaConverters._

	/* Extract "CASE value (WHEN A==value THEN a)* ELSE c END */
	object When {
		    def unapply[A <: TreeNode[A]](l: Seq[A]): Option[(A, Seq[(A, A)], A)] = {
		      if (l.length < 3)
		        None
		      else {
		        l match {
		          case e1 :: tail => {
		            val buffer = new collection.mutable.ArrayBuffer[(A, A)]()
		            def remains(rest: Seq[A]): Option[A] = {
		              rest match {
		                case a :: b :: tail => {
		                  buffer += ((a, b))
		                  remains(tail)
		                }
		                case a :: Nil => Some(a)
		                case _ => None
		              }
		            }
		
		            val e3 = remains(tail) match {
		              case Some(a) => a
		              case None => null.asInstanceOf[A]
		            }
		
		            Some((e1, buffer.toSeq, e3))
		          }
		          case _ => None
		        }
		      }
		    }
	}

	/* Extract "CASE (WHEN a THEN b)* [ELSE f] END"*/
	object Case {
		// TODO need to figure how to pattern matching with Array
		def unapply[A <: TreeNode[A]](l: Array[A]): Option[(Seq[(A, A)], A)] = {
			None
		}
		    def unapply[A <: TreeNode[A]](l: Seq[A]): Option[(Seq[(A, A)], A)] = {
		      if (l.length < 2)
		        None
		      else {
		        l match {
		          case e1 :: tail => {
		            val buffer = new collection.mutable.ArrayBuffer[(A, A)]()
		            def remains(rest: Seq[A]): Option[A] = {
		              rest match {
		                case a :: b :: tail => {
		                  buffer += ((a, b))
		                  remains(tail)
		                }
		                case a :: Nil => Some(a)
		                case _ => None
		              }
		            }
		
		            val e3 = remains(l) match {
		              case Some(a) => a
		              case None => null.asInstanceOf[A]
		            }
		
		            Some((buffer.toSeq, e3))
		          }
		          case Nil => None
		        }
		      }
		    }
	}

	protected def udfParamTypes(clazz: Class[_ <: UDF], children: Seq[DataType]) = {
		/*please refs org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
     * We need to figure out which UDF function(which may has several versions of "evaluate" method) 
     * is the most suitable per the function parameter list, and create the converter(s) if
     * necessary.
     * */
		import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption

		val udf = ReflectionUtils.newInstance(clazz, null).asInstanceOf[UDF]

		val udfMethod = udf.getResolver().getEvalMethod(children.map(_.typeInfo))
		udfMethod.setAccessible(true);

		// Create parameter converters
		val helper = new ConversionHelper(udfMethod, children.map(_.oi.asInstanceOf[OI]).toArray);

		var f4 = classOf[ConversionHelper].getDeclaredField("methodParameterTypes")
		f4.setAccessible(true)

		var isVariableLengthArgument = false // e.g concat() is with variable parameters
		// expected parameters OI (types)
		var methodParameterTypes = f4.get(helper).asInstanceOf[Array[Type]]

		// the last parameter also can be an array, the last parameter of UDF is the Array type
		// if it accept the variable length of parameters (e.g. UDFConcat)
		var lastParaElementType: Type = null
		if (methodParameterTypes.length > 0) {
			lastParaElementType = TypeInfoUtils.getArrayElementType(
				methodParameterTypes(methodParameterTypes.length - 1))
			if (null == lastParaElementType) {
				// not array;
				lastParaElementType = methodParameterTypes(methodParameterTypes.length - 1)
			} else {
				isVariableLengthArgument = true
			}
		}

		if (isVariableLengthArgument) {
			if (methodParameterTypes.length > children.length) {
				// UDF function signature doesn't match the input parameters;
				throw new CGAssertRuntimeException("wrong number of parameters for GUDF:" + clazz)
			}
		} else {
			if (methodParameterTypes.length != children.length) {
				// UDF function signature doesn't match the input parameters;
				throw new CGAssertRuntimeException("wrong number of parameters for GUDF:" + clazz)
			}
		}

		def extractOutputObjectInspector(inputOI: DataType, outputType: Type) = {
			TypeUtil.getDataType(if (outputType == classOf[Object])
				ObjectInspectorUtils.getStandardObjectInspector(inputOI.oi.asInstanceOf[OI], ObjectInspectorCopyOption.JAVA)
			else
				OIF.getReflectionObjectInspector(outputType, OIF.ObjectInspectorOptions.JAVA))
		}

		// get the output data type array
		val inputDT = for (ele <- children.zipWithIndex)
			yield extractOutputObjectInspector(ele._1,
			if (ele._2 >= methodParameterTypes.length - 1 && lastParaElementType != null)
				lastParaElementType
			else
				methodParameterTypes(ele._2))
		val outputDT = OIF.getReflectionObjectInspector(udfMethod.getGenericReturnType(), OIF.ObjectInspectorOptions.JAVA)

		(outputDT, inputDT)
	}

	protected def commonType(inputTypes: Seq[DataType]): DataType = {
		assert(inputTypes.length > 0)
		if (inputTypes.length == 1) {
			inputTypes(0)
		} else {
			def commonTypeInfo(t1: TypeInfo, t2: TypeInfo) = {
				// built-in binary udf requires all of the parameters to be in same type,
				if (t1 != t2) {
					if (t1 == TIF.stringTypeInfo ||
						t2 == TIF.stringTypeInfo) {
						// TODO, now we conform to Hive behavior, if not the same type, and one of the
						// type is string, then convert to double type info
						TIF.doubleTypeInfo
					} else {
						var compareType = FunctionRegistry.getCommonClass(t1, t2)
						// if can not find the common type, than default type info is double
						if (compareType == null) TIF.doubleTypeInfo else compareType
					}
				} else {
					t1
				}
			}
			val typeInfo = inputTypes.map(_.typeInfo).reduce((a, b) => { commonTypeInfo(a, b) })
			TypeUtil.getDataType(typeInfo)
		}
	}

	protected def convert(expectedTypes: Seq[DataType], nodes: Seq[TypedExprNode]): Seq[TypedExprNode] = {
		Seq.tabulate(nodes.length) { i => convert(expectedTypes(i), nodes(i)) }
	}

	/**
	 * Helper function to convert object to specified primitive type. 
	 */
	protected def convert(expectedType: DataType, node: TypedExprNode): TypedExprNode = {
		val inputType = node.outputDT
		
		if (inputType == expectedType) {
			node
		} else {
			TENConvertR2R(node, expectedType)
		}
	}

	def literal(obj: AnyRef, dt: DataType) = TENLiteral(obj, dt)

	def attribute(attr: String, child: TypedExprNode) = TENAttribute(attr, child)

	def gudf(clazz: Class[_ <: GenericUDF], children: Seq[TypedExprNode]) = {
		val n = TENGUDF(clazz, children)
		val outputType = n.outputDT
		if (ObjectInspectorUtils.isConstantObjectInspector(outputType.oi.asInstanceOf[OI]) &&
			n.isDeterministic && !n.isStateful) {
			val value = outputType.asInstanceOf[ConstantObjectInspector].getWritableConstantValue()
			literal(value, outputType)
		} else {
			TENGUDF(clazz, children.map(node => (node match {
				case _ @ (TENGUDF(_, _) | TENUDF(_, _)) => TENConvertW2D(node)
				case _ => TENConvertW2D(TENConvertR2W(node))
			}).asInstanceOf[TypedExprNode]))
		}
	}
	
	def udf(bridge: GenericUDFBridge, children: Seq[TypedExprNode]) = {
		val n = TENUDF(bridge, children)
		val outputType = n.outputDT
		if (ObjectInspectorUtils.isConstantObjectInspector(outputType.oi.asInstanceOf[OI]) &&
			n.isDeterministic && !n.isStateful) {
			val value = outputType.asInstanceOf[ConstantObjectInspector].getWritableConstantValue()
			literal(value, outputType)
		} else {
			val inputDT = udfParamTypes(bridge.getUdfClass(), children.map(_.outputDT))._2
			
			TENUDF(bridge, children.zip(inputDT).map(node => (node match {
				case _ @ (x @ (TENGUDF(_, _) | TENUDF(_, _)), toDT) => if(toDT == x.outputDT) {
					node
				} else {
					TENConvertR2W(TENConvertR2R(TENConvertW2R(x), toDT))
				}
				case _ @ (x, toDT) => if(toDT == x.outputDT) {
					TENConvertR2W(x)
				} else {
					TENConvertR2W(TENConvertR2R(x, toDT))
				}
			}).asInstanceOf[TypedExprNode]))
		}
	}

	def builtin(op: String, children: Seq[TypedExprNode], dt: DataType = null): TypedExprNode = {
		val inputTypes = children.map(_.outputDT)
		val expectType = commonType(inputTypes)
		val rewriteChildren = if (expectType != null)
			Seq.tabulate[TypedExprNode](children.length)(i => convert(expectType, children(i)))
		else
			Seq.tabulate[TypedExprNode](children.length)(i => literal(null, null))

		TENBuiltin(op, rewriteChildren, if (dt == null) expectType else dt)
	}

	def isnotnull(child: TypedExprNode) = TENBuiltin("isnotnull", Seq(child), TypeUtil.BooleanType, false)
	def isnull(child: TypedExprNode) = TENBuiltin("isnull", Seq(child), TypeUtil.BooleanType, false)
	def istrue(child: TypedExprNode) = TENBuiltin("istrue", Seq(convert(TypeUtil.BooleanType, child)), TypeUtil.BooleanType)
	def isfalse(child: TypedExprNode) = TENBuiltin("isfalse", Seq(convert(TypeUtil.BooleanType, child)), TypeUtil.BooleanType)
	def and(n1: TypedExprNode, n2: TypedExprNode) = branch(isfalse(n1), constantFalse, istrue(n2))
	def or(n1: TypedExprNode, n2: TypedExprNode) = branch(istrue(n1), constantTrue, istrue(n2))
	def between(children: Seq[TypedExprNode]) = {
		and(builtin(">=", Seq(children(0), children(1)), TypeUtil.BooleanType), builtin("<=", Seq(children(0), children(2)), TypeUtil.BooleanType))
	}
	def branch(branchIf: TypedExprNode, branchThen: TypedExprNode, branchElse: TypedExprNode): TypedExprNode = {
		val expectType = commonType(branchThen.outputDT :: branchElse.outputDT :: Nil)

		TENBranch(convert(TypeUtil.BooleanType, branchIf),
			if (expectType == branchThen.outputDT) branchThen else convert(expectType, branchThen),
			if (expectType == branchElse.outputDT) branchElse else convert(expectType, branchElse))
	}

	def branch(branchIfs: Seq[TypedExprNode], branchThens: Seq[TypedExprNode], branchElse: TypedExprNode): TypedExprNode = {
		assert(branchIfs.length == branchThens.length)

		val convertedIfs = branchIfs.map(convert(TypeUtil.BooleanType, _))
		val expectType = commonType((branchThens :+ branchElse).map(_.outputDT))

		val convertedBranchThens = branchThens.map(n => { convert(expectType, n) })
		val convertedBranchElse = convert(expectType, branchElse)

		convertedIfs.zip(convertedBranchThens).foldRight(branchElse)((a, b) => { branch(a._1, a._2, b) })
	}

	/**
	 * when
	 * "CASE WHEN a THEN b WHEN c THEN d [ELSE f] END"
	 * a and c should be boolean
	 */
	def branch_when(children: Seq[TypedExprNode]) = children match {
		case When(a, b, c) => branch(b.map(p => { TENFactory.builtin("==", Seq(a, p._1)) }), b.map(_._2), c)
		case _ => throw new CGAssertRuntimeException("wrong number of parameters in When")
	}

	/**
	 * case
	 * case value when A then a [when B then b] else c end
	 * if value == null then the value would be "c", if any, other wise null
	 */
	def branch_case(children: Seq[TypedExprNode]) = children match {
		case Case(b, c) => branch(b.map(_._1), b.map(_._2), c)
		case _ => throw new CGAssertRuntimeException("wrong number of parameters in Case")
	}

	def branch_if(children: Seq[TypedExprNode]) = if (children.length > 2) {
		branch(children(0), children(1), children(2))
	} else {
		branch(children(0), children(1), constantNull)
	}
}