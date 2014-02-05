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

import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.{StructField, StructObjectInspector}


/**
 * Used to serialize a row of data. It needs to be initialized with an object inspector
 * for the row.
 */
class HiveStructSerializer(val rowObjectInspector: StructObjectInspector) {

  def serialize(obj: Object): Array[Byte] = {
    outputByteBuffer.reset()
    var i = 0
    while (i < fields.size) {
      BinarySortableSerDe.serialize(
        outputByteBuffer,
        rowObjectInspector.getStructFieldData(obj, fields.get(i)),
        fields.get(i).getFieldObjectInspector(),
        false)
      i += 1
    }
    val bytes = new Array[Byte](outputByteBuffer.length)
    System.arraycopy(outputByteBuffer.getData(), 0, bytes, 0, outputByteBuffer.length)
    bytes
  }

  private val outputByteBuffer = new OutputByteBuffer
  private val fields: JList[_ <: StructField] = rowObjectInspector.getAllStructFieldRefs
}
