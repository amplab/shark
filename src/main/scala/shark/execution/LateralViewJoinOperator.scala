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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.plan.{LateralViewJoinDesc, SelectDesc}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}


/**
 * LateralViewJoin is used only for LATERAL VIEW explode, which adds a new row per array element
 * in the array to be exploded. Each new row contains one of the array elements in a new field.
 * Hive handles this by having two branches in its plan, then joining their output (see diagram in
 * LateralViewJoinOperator.java). We put all the explode logic here instead.
 */
class LateralViewJoinOperator extends NaryOperator[LateralViewJoinDesc] {

  @BeanProperty var conf: SelectDesc = _
  @BeanProperty var lvfOp: LateralViewForwardOperator = _
  @BeanProperty var lvfOIString: String = _
  @BeanProperty var udtfOp: UDTFOperator = _
  @BeanProperty var udtfOIString: String = _

  @transient var eval: Array[ExprNodeEvaluator] = _
  @transient var fieldOis: StructObjectInspector = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    // Get conf from Select operator beyond UDTF Op to get eval()
    conf = parentOperators.filter(_.isInstanceOf[UDTFOperator]).head
      .parentOperators.head.asInstanceOf[SelectOperator].desc

    udtfOp = parentOperators.filter(_.isInstanceOf[UDTFOperator]).head.asInstanceOf[UDTFOperator]
    udtfOIString = KryoSerializerToString.serialize(udtfOp.objectInspectors)
    lvfOp = parentOperators.filter(_.isInstanceOf[SelectOperator]).head.parentOperators.head
      .asInstanceOf[LateralViewForwardOperator]
    lvfOIString = KryoSerializerToString.serialize(lvfOp.objectInspectors)
  }

  override def initializeOnSlave() {
    lvfOp.objectInspectors = KryoSerializerToString.deserialize(lvfOIString)
    udtfOp.objectInspectors = KryoSerializerToString.deserialize(udtfOIString)

    // Get eval(), which will return array that needs to be exploded
    // eval doesn't exist when getColList() is null, but this happens only on select *'s,
    // which are not allowed within explode
    eval = conf.getColList().map(ExprNodeEvaluatorFactory.get(_)).toArray
    eval.foreach(_.initialize(objectInspectors.head))

    // Initialize UDTF operator so that we can call explode() later
    udtfOp.initializeOnSlave()
  }

  override def outputObjectInspector() = {
    val SELECT_TAG = 0
    val UDTF_TAG = 1
  
    val ois = new ArrayBuffer[ObjectInspector]()
    val fieldNames = desc.getOutputInternalColNames()

    // The output of the lateral view join will be the columns from the select
    // parent, followed by the column from the UDTF parent
    var soi = objectInspectors(SELECT_TAG).asInstanceOf[StructObjectInspector]

    for (sf <- soi.getAllStructFieldRefs()) {
      ois.add(sf.getFieldObjectInspector());
    }

    soi = objectInspectors(UDTF_TAG).asInstanceOf[StructObjectInspector]
    for (sf <- soi.getAllStructFieldRefs()) {
      ois.add(sf.getFieldObjectInspector());
    }

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, ois)
  }
  
  override def execute: RDD[_] = {
    // Execute LateralViewForwardOperator, bypassing Select / UDTF - Select
    // branches (see diagram in Hive's).
    val inputRDD = lvfOp.execute()

    Operator.executeProcessPartition(this, inputRDD)
  }

  def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    throw new Exception(
      "LateralViewJoinOperator.combineMultipleRdds() should never have been called.")
  }

  /** Per existing row, emit a new row with each value of the exploded array */
  override def processPartition(split: Int, iter: Iterator[_]) = {
    val lvfSoi = lvfOp.objectInspectors.head.asInstanceOf[StructObjectInspector]
    val lvfFields = lvfSoi.getAllStructFieldRefs()

    iter.flatMap { row =>
      var arrToExplode = eval.map(x => x.evaluate(row))
      val explodedRows = udtfOp.explode(arrToExplode)

      explodedRows.map { expRow =>
        val expRowArray = expRow.asInstanceOf[Array[java.lang.Object]]
        val joinedRow = new Array[java.lang.Object](lvfFields.size + expRowArray.length)

        // Add row fields from LateralViewForward
        var i = 0
        while (i < lvfFields.size) {
          joinedRow(i) = lvfSoi.getStructFieldData(row, lvfFields.get(i))
          i += 1
        }
        // Append element(s) from explode
        i = 0
        while (i < expRowArray.length) {
          joinedRow(i + lvfFields.size) = expRowArray(i)
          i += 1
        }
        joinedRow
      }
    }
  }
}


/**
 * Use Kryo to serialize udtfOp and lvfOp ObjectInspectors, then convert the Array[Byte]
 * to a String, since XML serialization of Bytes (for @BeanProperty keyword) is inefficient.
 */
object KryoSerializerToString {

  @transient val kryoSer = new SparkKryoSerializer(SparkEnv.get.conf)

  def serialize[T](o: T): String = {
    val bytes = kryoSer.newInstance().serialize(o).array()
    new String(Base64.encodeBase64(bytes))
  }

  def deserialize[T](byteString: String): T  = {
    val bytes = Base64.decodeBase64(byteString.getBytes())
    kryoSer.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}
