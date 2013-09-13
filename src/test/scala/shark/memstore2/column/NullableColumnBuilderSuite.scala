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

import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import org.scalatest.FunSuite

class NullableColumnBuilderSuite extends FunSuite {

  test("Empty column") {
    val c = new IntColumnBuilder()
    c.initialize(4)
    val b = c.build()
    // # of nulls
    assert(b.getInt() === 0)
    // column type
    assert(b.getInt() === INT.typeID)
    assert(b.getInt() === DefaultCompressionType.typeID)
    assert(!b.hasRemaining)
  }

  test("Buffer size auto growth") {
    val c = new StringColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    c.append(new Text("a"), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(null, oi)
    c.append(new Text("abc"), oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(new Text("efg"), oi)
    val b = c.build()
    b.position(4 + 4 * 4)
    val colType = b.getInt()
    assert(colType === STRING.typeID)
  }

  test("Null Strings") {
    val c = new StringColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    c.append(new Text("a"), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(null, oi)
    val b = c.build()

    // Number of nulls
    assert(b.getInt() === 2)

    // First null position is 1, and then 3
    assert(b.getInt() === 1)
    assert(b.getInt() === 3)

    // Column data type
    assert(b.getInt() === STRING.typeID)

    // Compression type
    assert(b.getInt() === DefaultCompressionType.typeID)

    // Data
    assert(b.getInt() === 1)
    assert(b.get() === 97)
    assert(b.getInt() === 1)
    assert(b.get() === 98)
  }

  test("Null Ints") {
    val c = new IntColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    c.append(123.asInstanceOf[Object], oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(56.asInstanceOf[Object], oi)
    val b = c.build()

    // # of nulls and null positions
    assert(b.getInt() === 2)
    assert(b.getInt() === 1)
    assert(b.getInt() === 2)

    // non nulls
    assert(b.getInt() === INT.typeID)
    assert(b.getInt() === DefaultCompressionType.typeID)
    assert(b.getInt() === 123)
  }

  test("Nullable Ints 2") {
    val c = new IntColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    Range(1, 1000).foreach { x =>
      c.append(x.asInstanceOf[Object], oi)
    }
    val b = c.build()
    // null count
    assert(b.getInt() === 0)
    // column type
    assert(b.getInt() === INT.typeID)
    // compression type
    assert(b.getInt() === DefaultCompressionType.typeID)
  }

  test("Null Longs") {
    val c = new LongColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.javaLongObjectInspector
    c.append(123L.asInstanceOf[Object], oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(56L.asInstanceOf[Object], oi)
    val b = c.build()

    // # of nulls and null positions
    assert(b.getInt() === 2)
    assert(b.getInt() === 1)
    assert(b.getInt() === 2)

    // non-nulls
    assert(b.getInt() === LONG.typeID)
    assert(b.getInt() === DefaultCompressionType.typeID)
    assert(b.getLong() === 123L)
  }

}
