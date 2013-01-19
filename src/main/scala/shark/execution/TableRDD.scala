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

package shark.execution

import java.util.{List => JavaList}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector

import shark.execution.serialization.KryoSerializer

import spark.{OneToOneDependency, RDD, Split, TaskContext}


/**
 * Wrapper for a row in TableRDD. This class provides a list of helper methods
 * to inspect the objects.
 */
class RowWrapper(
  val rawdata: Any,
  val colname2indexMap: Map[String, Int],
  val oi: StructObjectInspector) {

  def getBoolean(field: String): java.lang.Boolean = getBoolean(colname2indexMap(field))
  def getByte(field: String): java.lang.Byte = getByte(colname2indexMap(field))
  def getDouble(field: String): java.lang.Double = getDouble(colname2indexMap(field))
  def getFloat(field: String): java.lang.Float = getFloat(colname2indexMap(field))
  def getInt(field: String): java.lang.Integer = getInt(colname2indexMap(field))
  def getLong(field: String): java.lang.Long = getLong(colname2indexMap(field))
  def getShort(field: String): java.lang.Short = getShort(colname2indexMap(field))
  def getString(field: String): String = getString(colname2indexMap(field))
  def getTimestamp(field: String): java.sql.Timestamp = getTimestamp(colname2indexMap(field))

  def getBoolean(field: Int): java.lang.Boolean = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[BooleanObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Boolean]
  }

  def getByte(field: Int): java.lang.Byte = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[ByteObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Byte]
  }

  def getDouble(field: Int): java.lang.Double = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[DoubleObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Double]
  }

  def getFloat(field: Int): java.lang.Float = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[FloatObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Float]
  }

  def getInt(field: Int): java.lang.Integer = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[IntObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Integer]
  }

  def getLong(field: Int): java.lang.Long = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[LongObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Long]
  }

  def getShort(field: Int): java.lang.Short = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val obj = ref.getFieldObjectInspector.asInstanceOf[ShortObjectInspector].getPrimitiveJavaObject(data)
    obj.asInstanceOf[java.lang.Short]
  }

  def getString(field: Int): String = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[StringObjectInspector].getPrimitiveJavaObject(data)
  }

  def getTimestamp(field: Int): java.sql.Timestamp = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(data)
  }
}


case class TableRDD(
  val prev: RDD[Any],
  schema: JavaList[FieldSchema],
  @transient var oi: ObjectInspector)
  extends RDD[Any](prev.context) {

  /**
   * ObjectInspector is not Java serializable. We serialize it using Kryo and
   * and save it as a byte array. On slave nodes, we deserialize this byte
   * array to obtain the ObjectInspector object.
   */
  private val serializedObjectInspector: Array[Byte] = KryoSerializer.serialize(oi)
  @transient var structOi: StructObjectInspector = null

  /**
   * Maps the column name to column index.
   */
  private val colname2indexMap: Map[String, Int] =
    collection.immutable.Map() ++ schema.zipWithIndex.map {
      case(field, index) => (field.getName(), index)
    }

  /**
   * Instead of generating a normal MappedRDD, we overload map to generate a
   * MapPartitionsRDD because we need to initialize ObjectInspector on the
   * slaves. Unfortunately, we need to do this because ObjectInspector is not
   * Java serializable.
   */
  def mapRows[U: ClassManifest](f: RowWrapper => U): RDD[U] = {
    mapPartitions { partition =>
      initObjectInspector()
      partition.map(rawData => f(new RowWrapper(rawData, colname2indexMap, structOi)))
    }
  }

  override def splits = prev.splits

  override val dependencies = List(new OneToOneDependency(prev))

  override def compute(split: Split, context: TaskContext) = prev.iterator(split, context)

  /**
   * Initialize object inspector from the serializedObjectInspector.
   */
  private def initObjectInspector() {
    oi = KryoSerializer.deserialize[ObjectInspector](serializedObjectInspector)
    structOi = oi match {
      case soi: StructObjectInspector => soi
      case _ => throw new Exception("Only basic StructObjectInspector is supposed.")
    }
  }

}


