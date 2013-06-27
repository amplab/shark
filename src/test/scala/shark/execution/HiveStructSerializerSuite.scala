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

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.serde2.binarysortable.{HiveStructSerializer, HiveStructDeserializer}
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector,
  ObjectInspectorFactory, StandardListObjectInspector, StandardMapObjectInspector,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorUtils,
  PrimitiveObjectInspectorFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}

import org.scalatest.FunSuite


class HiveStructSerializerSuite extends FunSuite {

  test("Testing serializing a simple row") {
    val row1 = createRow(1, "test1")
    val row2 = createRow(2, "test2")
    val ser = new HiveStructSerializer(createObjectInspector)
    val deser = new HiveStructDeserializer(createObjectInspector)
    val deserRow1 = deser.deserialize(ser.serialize(row1))
    assert(row1.get(0).equals(deserRow1.get(0)))
    assert(row1.get(1).equals(deserRow1.get(1)))
  }

  def createObjectInspector(): StructObjectInspector = {
    val names = List("a", "b")
    val ois = List(
      createPrimitiveOi(classOf[java.lang.Integer]),
      createPrimitiveOi(classOf[String]))
    ObjectInspectorFactory.getStandardStructObjectInspector(names, ois)
  }

  def createRow(v1: Int, v2: String): JArrayList[Object] = {
    val row = new JArrayList[Object](2)
    row.add(new IntWritable(v1))
    row.add(new Text(v2))
    row
  }

  def createPrimitiveOi(javaClass: Class[_]): PrimitiveObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
      PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaClass(javaClass).primitiveCategory)
}
