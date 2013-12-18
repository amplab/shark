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

import java.util.{HashMap => JHashMap, List => JList, ArrayList => JArrayList}

import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.exec.{JoinUtil => HiveJoinUtil}
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.io.BytesWritable

import org.apache.spark.{CoGroupedRDD, HashPartitioner}
import org.apache.spark.rdd.RDD

import shark.execution.serialization.OperatorSerializationWrapper
import shark.io.MutableBytesWritable

class JoinOperator extends CommonJoinOperator[JoinDesc] with ReduceSinkTableDesc {

  @BeanProperty var valueTableDescMap: JHashMap[Int, TableDesc] = _
  @BeanProperty var keyTableDesc: TableDesc = _

  @transient var tagToValueSer: JHashMap[Int, Deserializer] = _
  @transient var keyDeserializer: Deserializer = _
  @transient var keyObjectInspector: StandardStructObjectInspector = _
//  @transient val tupleIterator = new this.TupleIterator[Array[AnyRef]]()

  override def initializeOnMaster() {
    super.initializeOnMaster()
    val descs = keyValueDescs()
    valueTableDescMap = new JHashMap[Int, TableDesc]
    valueTableDescMap ++= descs.map { case(tag, kvdescs) => (tag, kvdescs._2) }
    keyTableDesc = descs.head._2._1

    // Call initializeOnSlave to initialize the join filters, etc.
    initializeOnSlave()
    initializeJoinFilterOnMaster()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    
    tagToValueSer = new JHashMap[Int, Deserializer]
    valueTableDescMap foreach { case(tag, tableDesc) =>
      logDebug("tableDescs (tag %d): %s".format(tag, tableDesc))

      val deserializer = tableDesc.getDeserializerClass.newInstance()
      deserializer.initialize(null, tableDesc.getProperties())

      logDebug("value deser (tag %d): %s".format(tag, deserializer))
      tagToValueSer.put(tag, deserializer)
    }

    if (nullCheck) {
      keyDeserializer = keyTableDesc.getDeserializerClass.newInstance()
      keyDeserializer.initialize(null, keyTableDesc.getProperties())
      keyObjectInspector =
        keyDeserializer.getObjectInspector().asInstanceOf[StandardStructObjectInspector]
    }
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    // Determine the number of reduce tasks to run.
    var numReduceTasks = hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS)
    if (numReduceTasks < 1) {
      numReduceTasks = 1
    }

    // Turn the RDD into a map. Use a Java HashMap to avoid Scala's annoying
    // Some/Option. Add an assert for sanity check. If ReduceSink's join tags
    // are wrong, the hash entries might collide.
    val rddsJavaMap = new JHashMap[Int, RDD[_]]
    rddsJavaMap ++= rdds
    assert(rdds.size == rddsJavaMap.size, {
      logError("rdds.size (%d) != rddsJavaMap.size (%d)".format(rdds.size, rddsJavaMap.size))
    })

    val rddsInJoinOrder = order.map { inputIndex =>
      rddsJavaMap.get(inputIndex.byteValue.toInt).asInstanceOf[RDD[(ReduceKey, Any)]]
    }

    val part = new HashPartitioner(numReduceTasks)
    val cogrouped = new CoGroupedRDD[ReduceKey](
      rddsInJoinOrder.toSeq.asInstanceOf[Seq[RDD[(_, _)]]], part)

    val op = OperatorSerializationWrapper(this)

    cogrouped.mapPartitions { part =>
      op.initializeOnSlave()

      // reuse the BytesWritable
      val writable = new BytesWritable
      // reuse the key/value pair
      val objs = new Array[AnyRef](2)
      val nullSafes = op.conf.getNullSafes()

      val cp = new CartesianProduct[Array[AnyRef]](op.numTables)

      part.flatMap { case (k: ReduceKeyReduceSide, bufs: Array[_]) =>
        writable.set(k.byteArray, 0, k.length)
        objs(0) = op.keyDeserializer.deserialize(writable)

        // If nullCheck is false, we can skip deserializing the key.
        if (op.nullCheck &&
            SerDeUtils.hasAnyNullObject(
              objs(0).asInstanceOf[JList[_]],
              op.keyObjectInspector,
              nullSafes)) {
          bufs.iterator.zipWithIndex.flatMap { case (buf, label) =>
            val bufsNull = Array.fill(op.numTables)(Seq[Array[AnyRef]]())
            
            bufsNull(label) = buf.asInstanceOf[Seq[Array[Byte]]].map(data => {
              val bytes = data.asInstanceOf[Array[Byte]]
              writable.set(bytes, 0, bytes.length)
              op.computeJoinValues(label, writable, objs)
            })
            
//            op.tupleIterator.rebuilt(cp.product(bufsNull, op.joinConditions))
            op.generateTuples(cp.product(bufsNull, op.joinConditions))
          }
        } else {
          /* Within the same join key, the values in tables may look like:
           *   table1             table2            table3 ...
           *  a1(col1, col2..)   b1(cola, colb..)   c1(colx, coly..)
           *  a2(col1, col2..)                      c2(colx, coly..)
           *                                        c3(colx, coly..)
           * And the CartesianProduct result normally may looks like
           * a1, b1, c1
           * a1, b1, c2
           * a1, b1, c3
           * a2, b1, c1
           * a2, b1, c2
           * a2, b1, c3
           * 
           * And the "op.generateTuples" iterates above entries, and then feed into the join filters
           */
           val inputs = bufs.zipWithIndex.map { case (tblSeq: Any, tblIdx: Int) =>
              tblSeq.asInstanceOf[Seq[Any]].map { data =>
                val bytes = data.asInstanceOf[Array[Byte]]
                writable.set(bytes, 0, bytes.length)
                op.computeJoinValues(tblIdx, writable, objs)
              }
            }
           op.generateTuples(cp.product(inputs, op.joinConditions))
//          op.tupleIterator.rebuilt(cp.product(inputs, op.joinConditions))
        }
      }
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("JoinOperator.processPartition()")
  
  def computeJoinValues(tblIdx: Int, bytes: BytesWritable, objs: Array[AnyRef])
  : Array[AnyRef] = {
    val deser = tagToValueSer.get(tblIdx)
    val evaluators = joinVals(tblIdx)
    val ois = joinValuesObjectInspectors(tblIdx)
    val filters = joinFilters(tblIdx)
    val filterOIs = joinFilterObjectInspectors(tblIdx)
    objs(1) = deser.deserialize(bytes)
    
    JoinUtil.computeJoinValues(objs, evaluators, ois, filters, filterOIs, noOuterJoin)
  }
}
