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

import shark.execution.cg.CGAssertRuntimeException

class TENFactory {
	def create(name: String, desc: ExprNodeDesc, outputDT: DataType, input: TENInputRow): TENOutputField = {
		val expr = create(desc, input).toR
		// TODO need to think about how to identify the CGStruct / CGUnion etc.
		if (outputDT == null || outputDT == expr.outputDT) {
			TENOutputField(name, expr, expr.outputDT)
		} else {
			TENOutputField(name, TENConvertR2R(expr, outputDT), outputDT)
		}
	}
	
	def create(idx: Int, desc: ExprNodeDesc, outputDT: DataType, input: TENInputRow): TENOutputWritableField = {
		val expr = create(desc, input)
		// TODO need to think about how to identify the CGStruct / CGUnion etc.
		if(outputDT == expr.outputDT) {
			TENOutputWritableField(idx, expr.toW, expr.outputDT)
		} else {
		    TENOutputWritableField(idx, TENConvertR2R(expr.toR, outputDT).toW, expr.outputDT)
		}
	}	
	
    def create(desc: ExprNodeDesc, expectedDT: DataType, input: TENInputRow): TENOutputExpr = {
		val expr = create(desc, input).toR
		if(expr.outputDT == expectedDT) {
		  TENOutputExpr(expr)
		} else {
		  TENOutputExpr(TENConvertR2R(expr, expectedDT))
		}
	}

	private def create(node: ExprNodeGenericFuncDesc, input: TENInputRow): TypedExprNode = {
	  import scala.collection.JavaConverters._
	  val inputs = node.getChildren().asScala.map(create(_, input))
	  val genericUDF = node.getGenericUDF()
	  val folded = TENFactory.constantFolding(genericUDF, inputs)
	  val children = TENFactory.w2r(inputs)
	  
	  if(folded != null) {
	    folded
	  } else {
		genericUDF match {
			case x: GenericUDFOPAnd => {
				TENFactory.and(children(0), children(1)) // AND (&&)
			}
			case x: GenericUDFOPOr => {
				TENFactory.or(children(0), children(1)) // OR  (||)
			}
			case x: GenericUDFOPEqual => {
				TENFactory.builtin("==", children, TypeUtil.BooleanType) // =
			}
			case x: GenericUDFOPNotEqual => {
				TENFactory.builtin("!=", children, TypeUtil.BooleanType) // <> (!=)
			}
			case x: GenericUDFOPLessThan => {
				TENFactory.builtin("<", children, TypeUtil.BooleanType) // <
			}
			case x: GenericUDFOPEqualOrLessThan => {
				TENFactory.builtin("<=", children, TypeUtil.BooleanType) // <=
			}
			case x: GenericUDFOPGreaterThan => {
				TENFactory.builtin(">", children, TypeUtil.BooleanType) // >
			}
			case x: GenericUDFOPEqualOrGreaterThan => {
				TENFactory.builtin(">=", children, TypeUtil.BooleanType) // >=
			}
			case x: GenericUDFBetween => {
				TENFactory.between(children) // between 
			}
			case x: GenericUDFOPNull => {
				TENFactory.isnull(children(0)) // between // IS NULL
			}
			case x: GenericUDFOPNotNull => {
				TENFactory.isnotnull(children(0)) // IS NOT NULL
			}
			case x: GenericUDFCase => {
				TENFactory.branch_case(children)
			}
			case x: GenericUDFWhen => {
				val children = node.getChildren().asScala.map(create(_, input))
				TENFactory.branch_when(children)
			}
			case x: GenericUDFIf => {
				TENFactory.branch_if(children)
			}
			case x: GenericUDFBridge => {
				createUDF(node, x, input, inputs) // bridge
			}
			case x: GenericUDF => {
				TENFactory.gudf(x, TENFactory.r2w(inputs)) // call to the Generic UDF function
			}
		}
	  }
	}

	private def createUDF(node: ExprNodeGenericFuncDesc, x: GenericUDFBridge, input: TENInputRow, inputs: Seq[TypedExprNode]): TypedExprNode = {
		val udf = x.getUdfClass().newInstance()
		val children = TENFactory.w2r(inputs)
		udf match {
			case x: UDFOPPlus => {// +
				TENFactory.builtin("+", children) 
			}
			case x: UDFOPMinus => {// -
				TENFactory.builtin("-", children) 
			}
			case x: UDFOPMultiply => {// *
				TENFactory.builtin("*", children) 
			}
			case x: UDFOPDivide => {// /
				TENFactory.builtin("/", children) 
			}
			case x: UDFOPLongDivide => {// / to the long integer
				TENFactory.builtin("/", children) 
			}
			case x: UDFOPMod => {// %
				TENFactory.builtin("%", children) 
			}
			case x: UDFPosMod => {// pmod ((a % b) + b) % b
				TENFactory.builtin("%", 
					Seq(TENFactory.builtin("+", 
						Seq(TENFactory.builtin("%", children), children(1))), children(1))) 
			}
			case x: UDFOPBitAnd => {// &
				TENFactory.builtin("&", children) 
			}
			case x: UDFOPBitOr => {// |
				TENFactory.builtin("|", children) 
			}
			case x: UDFOPBitXor => {// ^
				TENFactory.builtin("^", children) 
			}
			case x: UDFOPBitNot => {// ~
				TENFactory.builtin("~", children) 
			}
			case x: UDFToBoolean => {
			  TENFactory.convert(TypeUtil.BooleanType, children(0))
			}
			case x: UDFToByte => {
			  TENFactory.convert(TypeUtil.ByteType, children(0))
			}
			case x: UDFToDouble => {
			  TENFactory.convert(TypeUtil.DoubleType, children(0))
			}
			case x: UDFToFloat => {
			  TENFactory.convert(TypeUtil.FloatType, children(0))
			}
			case x: UDFToInteger => {
			  TENFactory.convert(TypeUtil.IntegerType, children(0))
			}
			case x: UDFToLong => {
			  TENFactory.convert(TypeUtil.LongType, children(0))
			}
			case x: UDFToShort => {
			  TENFactory.convert(TypeUtil.ShortType, children(0))
			}
			case x: UDFToString => {
			  TENFactory.convert(TypeUtil.StringType, children(0))
			}
			// un-recognized UDF will resort to the TENUDF, which generates code for UDF invoking 
			case _ => {
			  val expectedDTs = TENFactory.expectedDataType(x, inputs)
			  val outputWritable = expectedDTs._1
			  val arguments = expectedDTs._2.zip(inputs).map(pair => {
			    val expectedDT = pair._1._1
			    val writable = pair._1._2
			    
			    val child = if(expectedDT == pair._2.outputDT) {
			      pair._2
			    } else {
			      TENConvertR2R(pair._2.toR, expectedDT)
			    }
			    
			    if(writable) child.toW else child.toR
			  })
			  TENFactory.udf(x, arguments, outputWritable) // still reusing the UDF
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
	private def create(desc: ExprNodeDesc, input: TENInputRow): TypedExprNode = {
		desc match {
			case x: ExprNodeGenericFuncDesc => create(x, input)
			case x: ExprNodeFieldDesc => TENFactory.attribute(x.getFieldName(), create(x.getDesc(), input))
			// TODO should iterate every nested type, currently only support the outer attribute
			case x: ExprNodeColumnDesc => TENInputRow(input.struct, x.getColumn())
			case x: ExprNodeConstantDesc => TENFactory.literal(x.getValue(), TypeUtil.getDataType(x.getTypeInfo()), false)
			case x: ExprNodeNullDesc => TENFactory.literal(null, TypeUtil.getDataType(x.getTypeInfo()), false)
		}
	}
}

object constantTrue extends TENLiteral(true, TypeUtil.BooleanType, false)
object constantFalse extends TENLiteral(false, TypeUtil.BooleanType, false)

object TENFactory {
	import java.lang.reflect.Type
	import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
	import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
	import org.apache.hadoop.util.ReflectionUtils
	import scala.collection.JavaConverters._

	/* Extract "CASE value (WHEN A==value THEN a)* ELSE c END */
	object Case {
		    def unapply[A <: TreeNode[A]](l: Seq[A]): Option[(A, Seq[(A, A)], A)] = {
		      if (l.length < 3)
		        None
		      else {
		        l match {
		          case Seq(e1, tail @ _*) => {
		            val buffer = new collection.mutable.ArrayBuffer[(A, A)]()
		            def remains(rest: Seq[A]): Option[A] = {
		              rest match {
		                case Seq(a, b, tail @ _*) => {
		                  buffer += ((a, b))
		                  remains(tail)
		                }
		                case Seq(a) => Some(a)
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
	object When {
		// TODO need to figure how to pattern matching with Array
		def unapply[A <: TreeNode[A]](l: Array[A]): Option[(Seq[(A, A)], A)] = {
			None
		}
		    def unapply[A <: TreeNode[A]](l: Seq[A]): Option[(Seq[(A, A)], A)] = {
		      if (l.length < 2)
		        None
		      else {
		        l match {
		          case Seq(e1, tail @ _*) => {
		            val buffer = new collection.mutable.ArrayBuffer[(A, A)]()
		            def remains(rest: Seq[A]): Option[A] = {
		              rest match {
		                case Seq(a, b, tail @ _*) => {
		                  buffer += ((a, b))
		                  remains(tail)
		                }
		                case Seq(a) => Some(a)
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

	def w2r(children: Seq[TypedExprNode]): Seq[TypedExprNode] = {
	  children.map(_.toR)
	}
	def r2w(children: Seq[TypedExprNode]): Seq[TypedExprNode] = {
	  children.map(_.toW)
	}
	
	protected def udfParamTypes(clazz: Class[_ <: UDF], children: Seq[DataType]) = {
		/*please refs org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge
     * We need to figure out which UDF function(which may has several versions of "evaluate" method) 
     * is the most suitable per the function parameter list, and create the converter(s) if
     * necessary.
     * */
		import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption

		val udf = ReflectionUtils.newInstance(clazz, null).asInstanceOf[UDF]

		val udfMethod = udf.getResolver().getEvalMethod(children.map(_.typeInfo).asJava)
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
			if (outputType == classOf[Object])
				(TypeUtil.getDataType(
				  ObjectInspectorUtils.getStandardObjectInspector(
				    inputOI.oi.asInstanceOf[OI], ObjectInspectorCopyOption.JAVA)), 
				true)
			else
				(TypeUtil.getDataType(
				   OIF.getReflectionObjectInspector(
				     outputType, OIF.ObjectInspectorOptions.JAVA)), 
				false)
		}

		// get the output data type array
		val inputDT = for (ele <- children.zipWithIndex)
			yield extractOutputObjectInspector(ele._1,
			if (ele._2 >= methodParameterTypes.length - 1 && lastParaElementType != null)
				lastParaElementType
			else
				methodParameterTypes(ele._2))
		val outputType = udfMethod.getGenericReturnType()
		val outputOI = OIF.getReflectionObjectInspector(outputType, OIF.ObjectInspectorOptions.JAVA)

		import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector
		// outputWritable / inputs ==> (isWritable) / [(inputDT, isWritable) ... ]
		(!outputOI.isInstanceOf[AbstractPrimitiveJavaObjectInspector], inputDT)
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
	def convert(expectedType: DataType, node: TypedExprNode): TypedExprNode = {
	  if(node == null) {
	    null
	  } else {
		val inputType = node.outputDT
		
		if (inputType == expectedType) {
			node
		} else {
			TENConvertR2R(node, expectedType)
		}
	  }
	}

	def literal(obj: AnyRef, dt: DataType, writable: Boolean) = TENLiteral(obj, if(dt != null) dt else TypeUtil.NullType, writable)

	def attribute(attr: String, child: TypedExprNode) = TENAttribute(attr, child)

	def constantFolding(genericUDF: GenericUDF, children: Seq[TypedExprNode]): TENLiteral = {
	  val n = TENGUDF(genericUDF, r2w(children))
	  val outputType = n.outputDT
	  if (outputType.constant && n.isDeterministic && !n.isStateful) {
			literal(outputType.constantValue, TypeUtil.standardize(outputType), true)
		} else {
			null
		}
	}
	
	def gudf(expr: GenericUDF, children: Seq[TypedExprNode]) = {
	  TENGUDF(expr, children.map(node => TENConvertW2D(node.toW)))
	}
	
	def expectedDataType(bridge: GenericUDFBridge, children: Seq[TypedExprNode]) = {
	  udfParamTypes(bridge.getUdfClass(), children.map(_.outputDT))
	}
	
	def udf(bridge: GenericUDFBridge, children: Seq[TypedExprNode], writable: Boolean) = {
		TENUDF(bridge, children, writable)
	}

	def builtin(op: String, children: Seq[TypedExprNode], dt: DataType = null): TypedExprNode = {
		val inputTypes = children.map(_.outputDT)
		val expectType = commonType(inputTypes)
		val rewriteChildren = if (expectType != null)
			Seq.tabulate[TypedExprNode](children.length)(i => convert(expectType, children(i)))
		else
			Seq.tabulate[TypedExprNode](children.length)(i => literal(null, TypeUtil.NullType, false))

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
		
		val expectType = commonType((branchThens :+ branchElse).filter(_!=null).map(_.outputDT))

		val convertedBranchThens = branchThens.map(n => { convert(expectType, n) })

		val convertedBranchElse = if(branchElse == null) literal(null, expectType, false) else convert(expectType, branchElse)

		convertedIfs.zip(convertedBranchThens).foldRight(convertedBranchElse)((a, b) => { branch(a._1, a._2, b) })
	}

	/**
	 * case
	 * CASE value WHEN A THEN a [WHEN B THEN b] ELSE c END
	 * a and c should be boolean
	 */
	def branch_case(children: Seq[TypedExprNode]) = children match {
		case Case(a, b, c) => branch(b.map(p => { TENFactory.builtin("==", Seq(a, p._1)) }), b.map(_._2), c)
		case _ => throw new CGAssertRuntimeException("wrong number of parameters in Case")
	}

	/**
	 * when
	 * CASE WHEN a THEN b WHEN c THEN d [ELSE f] END
	 * if value == null then the value would be "c", if any, other wise null
	 */
	def branch_when(children: Seq[TypedExprNode]) = children match {
		case When(b, c) => branch(b.map(_._1), b.map(_._2), c)
		case _ => throw new CGAssertRuntimeException("wrong number of parameters in When")
	}

	def branch_if(children: Seq[TypedExprNode]) = if (children.length > 2) {
		branch(children(0), children(1), children(2))
	} else {
		branch(children(0), children(1), null)
	}
}