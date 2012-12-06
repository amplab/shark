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

package shark.execution.serialization

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer
import spark.{JavaSerializer => SparkJavaSerializer}


class SerializationSuite extends FunSuite {

  test("Java serializing object inspectors") {

    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val ois = KryoSerializationWrapper(new ArrayBuffer[ObjectInspector])
    ois.value += oi

    val ser = new SparkJavaSerializer
    val bytes = ser.newInstance().serialize(ois)
    val desered = ser.newInstance()
      .deserialize[KryoSerializationWrapper[ArrayBuffer[ObjectInspector]]](bytes)

    assert(desered.head.getTypeName() === oi.getTypeName())
  }

  test("Java serializing operators") {

    import shark.execution.{FileSinkOperator => SharkFileSinkOperator}

    val operator = new SharkFileSinkOperator
    operator.localHconf = new org.apache.hadoop.hive.conf.HiveConf
    operator.localHiveOp = new org.apache.hadoop.hive.ql.exec.FileSinkOperator
    val opWrapped = OperatorSerializationWrapper(operator)

    val ser = new SparkJavaSerializer
    val bytes = ser.newInstance().serialize(opWrapped)
    val desered = ser.newInstance()
      .deserialize[OperatorSerializationWrapper[SharkFileSinkOperator]](bytes)

    assert(desered.value != null)
    assert(desered.value.localHconf != null)
    assert(desered.value.localHiveOp != null)
  }
}


