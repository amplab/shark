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

  def getBoolean(field: String): Boolean = getBoolean(colname2indexMap(field))
  def getByte(field: String): Byte = getByte(colname2indexMap(field))
  def getDouble(field: String): Double = getDouble(colname2indexMap(field))
  def getFloat(field: String): Float = getFloat(colname2indexMap(field))
  def getInt(field: String): Int = getInt(colname2indexMap(field))
  def getLong(field: String): Long = getLong(colname2indexMap(field))
  def getShort(field: String): Short = getShort(colname2indexMap(field))
  def getString(field: String): String = getString(colname2indexMap(field))

  def getBoolean(field: Int): Boolean = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[BooleanObjectInspector].get(data)
  }

  def getByte(field: Int): Byte = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[ByteObjectInspector].get(data)
  }

  def getDouble(field: Int): Double = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[DoubleObjectInspector].get(data)
  }

  def getFloat(field: Int): Float = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[FloatObjectInspector].get(data)
  }

  def getInt(field: Int): Int = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[IntObjectInspector].get(data)
  }

  def getLong(field: Int): Long = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[LongObjectInspector].get(data)
  }

  def getShort(field: Int): Short = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[ShortObjectInspector].get(data)
  }

  def getString(field: Int): String = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[StringObjectInspector].getPrimitiveJavaObject(data)
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


