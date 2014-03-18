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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption

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
  /**
   * Create single field in java primitive object for output
   */
  def create(name: String, desc: ExprNodeDesc, outputDT: DataType, input: TENInputRow)
  : TENOutputField = {
    val expr = create(desc, input).toR
    // TODO need to think about how to identify the CGStruct / CGUnion etc.
    if (outputDT == null || outputDT == expr.outputDT) {
      TENOutputField(name, expr, expr.outputDT)
    } else {
      TENOutputField(name, TENConvertR2R(expr, outputDT), outputDT)
    }
  }
  
  /**
   * Create single field in Writable object for output
   */
  def create(idx: Int, desc: ExprNodeDesc, outputDT: DataType, input: TENInputRow)
  : TENOutputWritableField = {
    val expr = create(desc, input)
    // TODO need to think about how to identify the CGStruct / CGUnion etc.
    if(outputDT == expr.outputDT) {
      TENOutputWritableField(idx, expr.toW, expr.outputDT)
    } else {
      TENOutputWritableField(idx, TENConvertR2R(expr.toR, outputDT).toW, expr.outputDT)
    }
  }  
  
  /**
   * Create expression in java primitive object as output, it's used in filter operator, which
   * requires the boolean as result.
   */
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
    
    if(folded != null) {
      folded
    } else {
      val children = TENFactory.w2r(inputs)
      genericUDF match {
        case x: GenericUDFOPAnd => {
          TENFactory.and(children(0), children(1)) // AND (&&)
        }
        case x: GenericUDFOPOr => {
          TENFactory.or(children(0), children(1)) // OR  (||)
        }
        case x: GenericUDFOPEqual => {
          TENFactory.builtin(TENBuiltin.OP_CMP_EQUAL, children) // =
        }
        case x: GenericUDFOPNotEqual => {
          TENFactory.builtin(TENBuiltin.OP_CMP_NOT_EQUAL, children) // <> (!=)
        }
        case x: GenericUDFOPLessThan => {
          TENFactory.builtin(TENBuiltin.OP_CMP_LESS, children) // <
        }
        case x: GenericUDFOPEqualOrLessThan => {
          TENFactory.builtin(TENBuiltin.OP_CMP_LESS_OR_EQUAL, children) // <=
        }
        case x: GenericUDFOPGreaterThan => {
          TENFactory.builtin(TENBuiltin.OP_CMP_GREATE, children) // >
        }
        case x: GenericUDFOPEqualOrGreaterThan => {
          TENFactory.builtin(TENBuiltin.OP_CMP_GREATE_OR_EQUAL, children) // >=
        }
        case x: GenericUDFBetween => {
          TENFactory.between(children) // between 
        }
        case x: GenericUDFOPNull => {
          TENFactory.isnull(children(0)) // IS NULL
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

  private def createUDF(node: ExprNodeGenericFuncDesc, x: GenericUDFBridge, 
    input: TENInputRow, inputs: Seq[TypedExprNode]): TypedExprNode = {
    val udf = x.getUdfClass().newInstance()
    val children = TENFactory.w2r(inputs)
    udf match {
      case x: UDFOPPlus => {// +
        TENFactory.builtin(TENBuiltin.OP_PLUS, children) 
      }
      case x: UDFOPMinus => {// -
        TENFactory.builtin(TENBuiltin.OP_MINUS, children) 
      }
      case x: UDFOPMultiply => {// *
        TENFactory.builtin(TENBuiltin.OP_MULTIPLY, children) 
      }
      case x: UDFOPDivide => {// /
        TENFactory.builtin(TENBuiltin.OP_DIVIDE, children) 
      }
      case x: UDFOPLongDivide => {// / to the long integer
        TENFactory.builtin(TENBuiltin.OP_DIVIDE, children) 
      }
      case x: UDFOPMod => {// %
        TENFactory.builtin(TENBuiltin.OP_MOD, children) 
      }
      case x: UDFPosMod => {// pmod ((a % b) + b) % b
        TENFactory.builtin(TENBuiltin.OP_MOD, 
          Seq(TENFactory.builtin(TENBuiltin.OP_PLUS, 
            Seq(TENFactory.builtin(TENBuiltin.OP_MOD, children), children(1))), children(1))) 
      }
      case x: UDFOPBitAnd => {// &
        TENFactory.builtin(TENBuiltin.OP_BIT_AND, children) 
      }
      case x: UDFOPBitOr => {// |
        TENFactory.builtin(TENBuiltin.OP_BIT_OR, children) 
      }
      case x: UDFOPBitXor => {// ^
        TENFactory.builtin(TENBuiltin.OP_BIT_XOR, children) 
      }
      case x: UDFOPBitNot => {// ~
        TENFactory.builtin(TENBuiltin.OP_BIT_NOT, children) 
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
        val arguments = expectedDTs.zip(inputs).map(pair => {
          val expectedDT = pair._1._1
          val writable = pair._1._2
          
          val child = if(expectedDT == pair._2.outputDT) {
            pair._2
          } else {
            TENConvertR2R(pair._2.toR, expectedDT)
          }
          
          if(writable) child.toW else child.toR
        })
        TENFactory.udf(x, arguments) // still reusing the UDF
      }
    }
  }

  /**
   * create the TypedExprNode per Hive expression description
   */
  private def create(desc: ExprNodeDesc, input: TENInputRow): TypedExprNode = {
    desc match {
      case x: ExprNodeGenericFuncDesc => create(x, input)
      case x: ExprNodeFieldDesc => 
        TENFactory.attribute(x.getFieldName(), create(x.getDesc(), input))
      // TODO should iterate every nested type, currently only support the outer attribute
      case x: ExprNodeColumnDesc => TENInputRow(input.struct, x.getColumn())
      case x: ExprNodeConstantDesc => 
        TENFactory.literal(x.getValue(), TypeUtil.getDataType(x.getTypeInfo()), false)
      case x: ExprNodeNullDesc => 
        TENFactory.literal(null, TypeUtil.getDataType(x.getTypeInfo()), false)
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
     */
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
      else {
        val oi = OIF.getReflectionObjectInspector(outputType, OIF.ObjectInspectorOptions.JAVA)
        (TypeUtil.getDataType(oi), TypeUtil.isWritable(oi))
      }
    }

    // get the output data type array
    val inputDT = for (ele <- children.zipWithIndex)
      yield extractOutputObjectInspector(ele._1,
      if (ele._2 >= methodParameterTypes.length - 1 && lastParaElementType != null)
        lastParaElementType
      else
        methodParameterTypes(ele._2))

    inputDT
  }
  
  
  protected def commonType(inputTypes: Seq[DataType]): DataType = if (inputTypes.length == 1) {
    inputTypes(0)
  } else {
    def commonTypeInfo(t1: TypeInfo, t2: TypeInfo) = {
      // built-in binary udf requires all of the parameters to be in same type,
      if (t1 != t2) {
        if (t1 == TIF.stringTypeInfo ||
          t2 == TIF.stringTypeInfo) {
          // if not the same type, and one of the type is string, then convert them to double
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

  protected def convert(expectedTypes: Seq[DataType], nodes: Seq[TypedExprNode])
  : Seq[TypedExprNode] = {
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

  def literal(obj: AnyRef, dt: DataType, writable: Boolean) = TENLiteral(obj, 
    if(dt != null) dt else TypeUtil.NullType, writable)

  def attribute(attr: String, child: TypedExprNode) = TENAttribute(attr, child)

  def constantFolding(genericUDF: GenericUDF, children: Seq[TypedExprNode]): TENLiteral = {
    val outputType = TypeUtil.getDataType(TypedExprNode.initializeGUDF(genericUDF, children))
    
    // if the output is constant, and doens't contain any non-deterministic expr in its sub exprs, 
    // stateful expression is also can not be folded, cause we have to recompute it in every row.
    // stateful expression like "row_sequence()"
    // non deterministic expression like "rand()"
    if (outputType.constant &&
      TypedExprNode.deterministic(genericUDF, children) && 
       (!TypedExprNode.stateful(genericUDF))) {
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
  
  def udf(bridge: GenericUDFBridge, children: Seq[TypedExprNode]) = {
    TENUDF(bridge, children)
  }

  def builtin(op: TENBuiltin.OP, children: Seq[TypedExprNode]): TypedExprNode = {
    val inputTypes = children.map(_.outputDT)
    val expectType = op.parameterDataType(commonType(inputTypes))
    val rewriteChildren = if (expectType != null)
      Seq.tabulate[TypedExprNode](children.length)(i => convert(expectType, children(i)))
    else
      Seq.tabulate[TypedExprNode](children.length)(i => literal(null, TypeUtil.NullType, false))

    TENBuiltin(op, rewriteChildren, op.resultDataType(expectType))
  }

  def isnotnull(child: TypedExprNode) = 
    TENBuiltin(TENBuiltin.OP_COND_ISNOTNULL, Seq(child), TypeUtil.BooleanType, false)
    
  def isnull(child: TypedExprNode) = 
    TENBuiltin(TENBuiltin.OP_COND_ISNULL, Seq(child), TypeUtil.BooleanType, false)
    
  def istrue(child: TypedExprNode) = 
    TENBuiltin(TENBuiltin.OP_COND_ISTRUE, 
      Seq(convert(TypeUtil.BooleanType, child)), TypeUtil.BooleanType)
    
  def isfalse(child: TypedExprNode) = 
    TENBuiltin(TENBuiltin.OP_COND_ISFALSE, 
      Seq(convert(TypeUtil.BooleanType, child)), TypeUtil.BooleanType)
    
  def and(n1: TypedExprNode, n2: TypedExprNode) = branch(isfalse(n1), constantFalse, istrue(n2))
  
  def or(n1: TypedExprNode, n2: TypedExprNode) = branch(istrue(n1), constantTrue, istrue(n2))
  
  def between(children: Seq[TypedExprNode]) = {
    // between contains 4 elements: for example: where a not between b and c
    // element 1 is the literal value represent if the range invert or not.
    // element 2 is the feed value
    // element 3 is the left boundary
    // element 4 is the right boundary
    assert(children(0).isInstanceOf[TENLiteral])
    if(children(0).asInstanceOf[TENLiteral].obj == false) {
      // not invert
      and(builtin(TENBuiltin.OP_CMP_GREATE_OR_EQUAL, Seq(children(1), children(2))), 
        builtin(TENBuiltin.OP_CMP_LESS_OR_EQUAL, Seq(children(1), children(3))))
    } else {
      // invert
      or(builtin(TENBuiltin.OP_CMP_LESS, Seq(children(1), children(2))), 
        builtin(TENBuiltin.OP_CMP_GREATE, Seq(children(1), children(3))))
    }
  }
  def branch(branchIf: TypedExprNode, branchThen: TypedExprNode, branchElse: TypedExprNode)
  : TypedExprNode = {
    val expectType = commonType(branchThen.outputDT :: branchElse.outputDT :: Nil)

    TENBranch(convert(TypeUtil.BooleanType, branchIf),
      if (expectType == branchThen.outputDT) branchThen else convert(expectType, branchThen),
      if (expectType == branchElse.outputDT) branchElse else convert(expectType, branchElse))
  }

  def branch(branchIfs: Seq[TypedExprNode], branchThens: Seq[TypedExprNode], 
      branchElse: TypedExprNode): TypedExprNode = {
    assert(branchIfs.length == branchThens.length)

    val convertedIfs = branchIfs.map(convert(TypeUtil.BooleanType, _))
    
    val expectType = commonType((branchThens :+ branchElse).filter(_!=null).map(_.outputDT))

    val convertedBranchThens = branchThens.map(n => { convert(expectType, n) })

    val convertedBranchElse = if(branchElse == null) {
      literal(null, expectType, false) 
    } else {
      convert(expectType, branchElse)
    }

    convertedIfs.zip(convertedBranchThens).foldRight(convertedBranchElse)((a, b) => { 
      branch(a._1, a._2, b) 
    })
  }

  /**
   * case
   * CASE value WHEN A THEN a [WHEN B THEN b] ELSE c END
   * a and c should be boolean
   */
  def branch_case(children: Seq[TypedExprNode]) = children match {
    case Case(a, b, c) => branch(b.map(p => { 
      TENFactory.builtin(TENBuiltin.OP_CMP_EQUAL, Seq(a, p._1))
    }), b.map(_._2), c)
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

/**
 * Create TEN from Hive ExprNodeDesc
 */
object HiveTENFactory {
  def create(names: Seq[String],
    exprs: Seq[ExprNodeDesc],
    input: CGStruct,
    output: CGStruct = null): TENOutputRow = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input, null)

    val fields: Seq[TENOutputField] = if (output != null) {
      // sweep the constant expression in the root
      output.fields.zip(exprs).filter((entry) => !(entry._1.constant)).map((entry) => {
        factory.create(entry._1.oiName, entry._2, entry._1, rowInput)
      })
    } else {
      // sweep the constant expression in the root
      import org.apache.hadoop.hive.ql.plan.{ ExprNodeConstantDesc, ExprNodeNullDesc }
      names.zip(exprs).filter((entry) => {
        !(entry._2.isInstanceOf[ExprNodeConstantDesc] || entry._2.isInstanceOf[ExprNodeNullDesc])
      }).map((entry) => factory.create(entry._1, entry._2, null, rowInput))
    }

    import scala.collection.JavaConversions._

    val dt = if (output == null) {
      TypeUtil.getDataType(
        OIF.getStandardStructObjectInspector(
          fields.map(_.attr),
          fields.map(_.outputDT.oi.asInstanceOf[OI]))).asInstanceOf[CGStruct]
    } else {
      output
    }

    TENOutputRow(fields, dt)
  }

  def create(exprs: Seq[ExprNodeDesc], input: CGStruct, output: CGStruct): TENOutputRow = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input, null)

    val fields = exprs.zipWithIndex.map((entry) => {
      factory.create(entry._2, entry._1, output.fields(entry._2), rowInput)
    })

    TENOutputRow(fields, null)
  }

  def create(filter: ExprNodeDesc, expectedDT: DataType, input: CGStruct): TENOutputExpr = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input, null)

    factory.create(filter, expectedDT, rowInput)
  }
}

/**
 * Create / Initialize EEN from TEN
 */
object TENInstance {
  def transform(node: TypedExprNode, ctx: CGExprContext) = {
    val rule = new TEN2EEN()
    val rotated = rule.rotate(node, null, false)
    val row = rule.eos(rotated, new PathNodeContext(), null)

    row.initialAll(ctx)

    row
  }
}
