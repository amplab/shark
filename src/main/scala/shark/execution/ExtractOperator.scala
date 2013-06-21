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

import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{ExtractOperator => HiveExtractOperator}
import org.apache.hadoop.hive.ql.plan.{ExtractDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.io.BytesWritable

import spark.RDD

import shark.SharkConfVars
import shark.execution.cg.CGEvaluatorFactory


class ExtractOperator extends UnaryOperator[HiveExtractOperator] with HiveTopOperator {

  @BeanProperty var conf: ExtractDesc = _
  @BeanProperty var valueTableDesc: TableDesc = _
  @BeanProperty var localHconf: HiveConf = _
  @BeanProperty var useCG = true

  @transient var eval: ExprNodeEvaluator = _
  @transient var valueDeser: Deserializer = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
    localHconf = super.hconf
    valueTableDesc = keyValueTableDescs.values.head._2
    useCG = SharkConfVars.getBoolVar(super.hconf, SharkConfVars.EXPR_CG)
  }

  override def initializeOnSlave() {
    eval = CGEvaluatorFactory.get(conf.getCol, useCG)
    eval.initialize(objectInspector)
    valueDeser = valueTableDesc.getDeserializerClass().newInstance()
    valueDeser.initialize(localHconf, valueTableDesc.getProperties())
  }

  override def preprocessRdd(rdd: RDD[_]): RDD[_] = {
    // TODO: hasOrder and limit should really be made by optimizer.
    val hasOrder = parentOperator match {
      case op: ReduceSinkOperator =>
        op.getConf.getOrder != null && !op.getConf.getOrder.isEmpty
      case _ => false
    }

    // Whether to consolidate all data to one partition.
    val consolidate = parentOperator match {
      case op: ReduceSinkOperator => op.getConf.getNumReducers == 1
      case _ => false
    }

    // If no limit is set, -1 is returned.
    val limit: Int =
      if (childOperators.size == 1) {
        childOperators.head match {
          case op: LimitOperator => op.limit
          case _ => -1
        }
      } else {
        -1
      }

    if (hasOrder && limit >= 0) {
      if (consolidate) {
        // Example: SELECT * FROM table ORDER BY col LIMIT 10;
        // Need to make a copy of each row. Otherwise, the rows are reused so topK won't work.
        val clonedRdd: RDD[(ReduceKey, BytesWritable)] =
          rdd.asInstanceOf[RDD[(ReduceKeyMapSide, BytesWritable)]].map { case(k, v) =>
            val value = new BytesWritable
            value.set(v.getBytes, 0, v.getLength)
            (k.createDeepCopy(), value)
          }
        RDDUtils.topK(clonedRdd, limit)
      } else {
        val distributedRdd = RDDUtils.repartition(rdd.asInstanceOf[RDD[(ReduceKey, Any)]],
          new ReduceKeyPartitioner(rdd.partitions.length))
        // Don't need to make a copy of each row because after a shuffle (repartition),
        // rows are not reused.
        RDDUtils.partitionTopK(distributedRdd, limit)
      }
    } else if (hasOrder && limit < 0) {
      if (consolidate) {
        rdd match {
          case r: RDD[(ReduceKey, Any)] => RDDUtils.sortByKey(r)
          case _ => rdd
        }
      } else {
        val clusteredRdd = RDDUtils.repartition(rdd.asInstanceOf[RDD[(ReduceKey, Any)]],
          new ReduceKeyPartitioner(rdd.partitions.length))
        clusteredRdd.mapPartitions { partition =>
          partition.toSeq.sortWith((x, y) => x._1.compareTo(y._1) < 0).iterator
        }
      }
    } else { // i.e. !hasOrder
      // Example for consolidate = true:
      //   SELECT count(*) FROM (SELECT * FROM table LIMIT 10) tbl;
      val numParts = if (consolidate) 1 else rdd.partitions.length
      RDDUtils.repartition(rdd.asInstanceOf[RDD[(ReduceKey, Any)]],
        new ReduceKeyPartitioner(numParts))
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    // TODO: use MutableBytesWritable to avoid the array copy.
    val writable = new BytesWritable
    iter map {
      case (key, value: Array[Byte]) => {
        writable.set(value, 0, value.length)
        valueDeser.deserialize(writable)
      }
      case unrecognizedObj => {
        throw new RuntimeException("Did not find key, value pair: " + unrecognizedObj.toString)
      }
    }
  }
}

