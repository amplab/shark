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

package shark.memstore2.column

import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text


import shark.execution.serialization.KryoSerializer
import shark.memstore2.column.ColumnStats._


class BooleanColumnBuilder extends DefaultColumnBuilder[Boolean](new BooleanColumnStats(), BOOLEAN)

class IntColumnBuilder extends DefaultColumnBuilder[Int](new IntColumnStats(), INT)

class LongColumnBuilder extends DefaultColumnBuilder[Long](new LongColumnStats(), LONG)

class FloatColumnBuilder extends DefaultColumnBuilder[Float](new FloatColumnStats(), FLOAT)

class DoubleColumnBuilder extends DefaultColumnBuilder[Double](new DoubleColumnStats(), DOUBLE)

class StringColumnBuilder extends DefaultColumnBuilder[Text](new StringColumnStats(), STRING)

class ByteColumnBuilder extends DefaultColumnBuilder[Byte](new ByteColumnStats(), BYTE)

class ShortColumnBuilder extends DefaultColumnBuilder[Short](new ShortColumnStats(), SHORT)

class TimestampColumnBuilder
  extends DefaultColumnBuilder[Timestamp](new TimestampColumnStats(), TIMESTAMP)

class BinaryColumnBuilder extends DefaultColumnBuilder[BytesWritable](new NoOpStats(), BINARY)

class VoidColumnBuilder extends DefaultColumnBuilder[Void](new NoOpStats(), VOID)

/**
 * Generic columns that we can serialize, including maps, structs, and other complex types.
 */
class GenericColumnBuilder(oi: ObjectInspector)
  extends DefaultColumnBuilder[ByteStream.Output](new NoOpStats(), GENERIC) {

  // Complex data types cannot be null. Override the initialize in NullableColumnBuilder.
  override def initialize(initialSize: Int, columnName: String): ByteBuffer = {
    val buffer = super.initialize(initialSize, columnName)
    val objectInspectorSerialized = KryoSerializer.serialize(oi)
    buffer.putInt(objectInspectorSerialized.size)
    buffer.put(objectInspectorSerialized)
    buffer
  }
}
