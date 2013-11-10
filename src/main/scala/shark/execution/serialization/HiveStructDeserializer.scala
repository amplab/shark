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

package org.apache.hadoop.hive.serde2.binarysortable

// Putting it in this package so it can access the package level visible function
// static void BinarySortableSerDe.serialize(OutputByteBuffer, Object, ObjectInspector, boolean)

import java.io.IOException
import java.util.{ArrayList => JArrayList}

import org.apache.hadoop.hive.serde2.SerDeException
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, TypeInfoUtils}


/**
 * Used to deserialize a row of data. It needs to be initialized with an object inspector
 * for the row.
 */
class HiveStructDeserializer(val rowObjectInspector: StructObjectInspector) {

  def deserialize(bytes: Array[Byte]): JArrayList[Object] = {
    inputByteBuffer.reset(bytes, 0, bytes.length)
    try {
      var i = 0
      while (i < types.size) {
        reusedRow.set(i,
          BinarySortableSerDe.deserialize(inputByteBuffer, types(i), false, reusedRow.get(i)))
        i += 1
      }
    } catch{
      case e: IOException =>  throw new SerDeException(e)
    }
    reusedRow
  }

  private val inputByteBuffer = new InputByteBuffer
  private val types = Array.tabulate[TypeInfo](rowObjectInspector.getAllStructFieldRefs.size) { i =>
    TypeInfoUtils.getTypeInfoFromObjectInspector(
      rowObjectInspector.getAllStructFieldRefs.get(i).getFieldObjectInspector)
  }

  private val reusedRow: JArrayList[Object] = {
    val row = new JArrayList[Object](rowObjectInspector.getAllStructFieldRefs.size())
    (0 until rowObjectInspector.getAllStructFieldRefs.size).foreach(i => row.add(null))
    row
  }
}
