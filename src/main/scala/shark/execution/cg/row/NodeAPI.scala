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

import org.apache.hadoop.io.Writable

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.{ TypeInfoFactory => TIF }

import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import shark.execution.cg.SetDeferred
import shark.execution.cg.SetRaw
import shark.execution.cg.SetWritable
import shark.execution.cg.CGNotSupportDataTypeRuntimeException

/**
 * Utilities about the DataType (which is about CGField and ObjectInspector)
 */
object TypeUtil {
  private val map = scala.collection.mutable.Map[TypeInfo, DataType]()

  val NullType = new CGNull(POIF.writableVoidObjectInspector, null, -1)
  val StringType = new CGPrimitiveString(POIF.writableStringObjectInspector, null, -1)
  val BinaryType = new CGPrimitiveBinary(POIF.writableBinaryObjectInspector, null, -1)
  val IntegerType = new CGPrimitiveInt(POIF.writableIntObjectInspector, null, -1)
  val BooleanType = new CGPrimitiveBoolean(POIF.writableBooleanObjectInspector, null, -1)
  val FloatType = new CGPrimitiveFloat(POIF.writableFloatObjectInspector, null, -1)
  val DoubleType = new CGPrimitiveDouble(POIF.writableDoubleObjectInspector, null, -1)
  val LongType = new CGPrimitiveLong(POIF.writableLongObjectInspector, null, -1)
  val ByteType = new CGPrimitiveByte(POIF.writableByteObjectInspector, null, -1)
  val ShortType = new CGPrimitiveShort(POIF.writableShortObjectInspector, null, -1)
  val TimestampType = new CGPrimitiveTimestamp(POIF.writableTimestampObjectInspector, null, -1)

  register(NullType)
  register(StringType)
  register(BinaryType)
  register(IntegerType)
  register(BooleanType)
  register(FloatType)
  register(DoubleType)
  register(LongType)
  register(ByteType)
  register(ShortType)
  register(TimestampType)

  // TODO need to support the non-primitive data type, which may require creating the new
  // object inspector(union / struct)
  def register(dt: DataType) {
    map += (dt.typeInfo -> dt)
  }

  def getSetWritableClass(): Class[_] = classOf[SetWritable]
  def getDeferredObjectClass(): Class[_] = classOf[SetDeferred]
  def getSetRawClass(): Class[_] = classOf[SetRaw]

  def getDataType(ti: TypeInfo): DataType = map.get(ti) match {
    case Some(x) => x
    case None => throw new CGNotSupportDataTypeRuntimeException(ti)
  }

  def getTypeInfo(oi: OI): TypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(oi)

  def getDataType(oi: OI): DataType = if (oi.isInstanceOf[PrimitiveObjectInspector] &&
    !oi.isInstanceOf[ConstantObjectInspector])
    getDataType(getTypeInfo(oi))
  else
    CGField.create(oi, null)

  def isWritable(oi: OI): Boolean = !oi.isInstanceOf[AbstractPrimitiveJavaObjectInspector]

  def isWritable(dt: DataType): Boolean = isWritable(dt.oi)

  def standardize(dt: DataType) = getDataType(getTypeInfo(dt.oi))

  def dtToString(dt: DataType): String = {
    // TODO, need to support the HiveDecimalObjectInspector
    val oi = dt.oi
    if (oi.isInstanceOf[PrimitiveObjectInspector] && !oi.isInstanceOf[HiveDecimalObjectInspector]) {
      dt.typedOIClassName
    } else {
      throw new CGNotSupportDataTypeRuntimeException(dt)
    }
  }

  def assertDataType(dt: DataType) {
    if (dt.isInstanceOf[CGUnion] ||
      dt.isInstanceOf[CGStruct] ||
      dt.isInstanceOf[CGMap] ||
      dt.isInstanceOf[CGList]) {
      throw new CGNotSupportDataTypeRuntimeException(dt)
    }
  }
}

// TODO the TreeNode API was from catalyst, will merge catalyst in the near future
abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  self: BaseType with Product =>

  /** Returns a Seq of the children of this node */
  def children: Seq[BaseType]
}

/**
 * A [[TreeNode]] with no children.
 */
trait LeafNode[BaseType <: TreeNode[BaseType]] {
  def children = Nil
}

/**
 * A [[TreeNode]] with a single [[child]].
 */
trait UnaryNode[BaseType <: TreeNode[BaseType]] {
  def child: BaseType
  def children = child :: Nil
}

abstract class ExprNode[NodeType <: TreeNode[NodeType]] extends TreeNode[NodeType] {
  self: NodeType with Product =>
}
