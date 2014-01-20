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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.plan.UDTFDesc
import org.apache.hadoop.hive.ql.udf.generic.Collector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField


class UDTFOperator extends UnaryOperator[UDTFDesc] {

  @BeanProperty var conf: UDTFDesc = _

  @transient var objToSendToUDTF: Array[java.lang.Object] = _
  @transient var soi: StandardStructObjectInspector = _
  @transient var inputFields: JavaList[_ <: StructField] = _
  @transient var collector: UDTFCollector = _
  @transient var outputObjInspector: ObjectInspector = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    
    conf = desc
    
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    collector = new UDTFCollector
    conf.getGenericUDTF().setCollector(collector)

    // Make an object inspector [] of the arguments to the UDTF
    soi = objectInspectors.head.asInstanceOf[StandardStructObjectInspector]
    inputFields = soi.getAllStructFieldRefs()

    val udtfInputOIs = inputFields.map { case inputField =>
      inputField.getFieldObjectInspector()
    }.toArray

    objToSendToUDTF = new Array[java.lang.Object](inputFields.size)
    outputObjInspector = conf.getGenericUDTF().initialize(udtfInputOIs)
  }

  override def outputObjectInspector() = outputObjInspector

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
    iter.flatMap { row =>
      explode(row)
    }
  }

  def explode[T](row: T): ArrayBuffer[java.lang.Object] = {
    (0 until inputFields.size).foreach { case i =>
      objToSendToUDTF(i) = soi.getStructFieldData(row, inputFields.get(i))
    }
    conf.getGenericUDTF().process(objToSendToUDTF)
    collector.collectRows()
  }
}

class UDTFCollector extends Collector {

  var collected = new ArrayBuffer[java.lang.Object]

  override def collect(input: java.lang.Object) {
    // We need to clone the input here because implementations of
    // GenericUDTF reuse the same object. Luckily they are always an array, so
    // it is easy to clone.
    collected += input.asInstanceOf[Array[_]].clone
  }

  def collectRows() = {
    val toCollect = collected
    collected = new ArrayBuffer[java.lang.Object]
    toCollect
  }

}
