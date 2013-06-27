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

package shark.api

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._


/**
 * Wrapper for a row in TableRDD. This class provides a list of helper methods
 * to inspect the objects. Most get methods return Java boxed objects rather than
 * primitives because the values can be null in SQL.
 */
class Row(val rawdata: Any, val colname2indexMap: Map[String, Int], val oi: StructObjectInspector) {

  def apply(field: String): Object = apply(colname2indexMap(field))

  def apply(field: Int): Object = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    ref.getFieldObjectInspector.asInstanceOf[PrimitiveObjectInspector].getPrimitiveJavaObject(data)
  }

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
    val foi = ref.getFieldObjectInspector.asInstanceOf[BooleanObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Boolean]
  }

  def getByte(field: Int): java.lang.Byte = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val foi = ref.getFieldObjectInspector.asInstanceOf[ByteObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Byte]
  }

  def getDouble(field: Int): java.lang.Double = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val foi = ref.getFieldObjectInspector.asInstanceOf[DoubleObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Double]
  }

  def getFloat(field: Int): java.lang.Float = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val foi = ref.getFieldObjectInspector.asInstanceOf[FloatObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Float]
  }

  def getInt(field: Int): java.lang.Integer = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val foi = ref.getFieldObjectInspector.asInstanceOf[IntObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Integer]
  }

  def getLong(field: Int): java.lang.Long = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val foi = ref.getFieldObjectInspector.asInstanceOf[LongObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Long]
  }

  def getShort(field: Int): java.lang.Short = {
    val ref = oi.getAllStructFieldRefs.get(field)
    val data = oi.getStructFieldData(rawdata, ref)
    val foi = ref.getFieldObjectInspector.asInstanceOf[ShortObjectInspector]
    foi.getPrimitiveJavaObject(data).asInstanceOf[java.lang.Short]
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
