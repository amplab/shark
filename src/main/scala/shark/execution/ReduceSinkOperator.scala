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

import java.util.ArrayList
import java.util.Random

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.exec.{ReduceSinkOperator => HiveReduceSinkOperator}
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.{ReduceSinkDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, SerDe}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  ObjectInspectorUtils}
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion
import org.apache.hadoop.io.{BytesWritable, Text}

import shark.SharkEnvSlave


/**
 * Converts a collection of rows into key, value pairs. This is the
 * upstream operator for joins and groupbys.
 */
class ReduceSinkOperator extends UnaryOperator[HiveReduceSinkOperator] {

  @BeanProperty var conf: ReduceSinkDesc = _

  // The evaluator for key columns. Key columns decide the sort/hash order on
  // the reducer side. Key columns are passed to the reducer in the "key".
  @transient var keyEval: Array[ExprNodeEvaluator] = _

  // The evaluator for the value columns. Value columns are passed to reducers
  // in the "value".
  @transient var valueEval: Array[ExprNodeEvaluator] = _

  // The evaluator for the partition columns (used in CLUSTER BY and
  // DISTRIBUTE BY in Hive). Partition columns decide the reducer that the
  // current row goes to. Partition columns are not passed to reducers.
  @transient var partitionEval: Array[ExprNodeEvaluator] = _

  @transient var keySer: SerDe = _
  @transient var valueSer: SerDe = _
  @transient var keyObjInspector: ObjectInspector = _
  @transient var valObjInspector: ObjectInspector = _
  @transient var partitionObjInspectors: Array[ObjectInspector] = _

  override def getTag() = conf.getTag()

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }

  override def initializeOnSlave() {
    initializeOisAndSers(conf, objectInspector)
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    if (conf.getDistinctColumnIndices().size() == 0) {
      processPartitionNoDistinct(iter)
    } else {
      processPartitionDistinct(iter)
    }
  }

  def initializeDownStreamHiveOperator() {

    conf = hiveOp.getConf()

    // Note that we get input object inspector from hiveOp rather than Shark's
    // objectInspector because initializeMasterOnAll() hasn't been invoked yet.
    initializeOisAndSers(conf, hiveOp.getInputObjInspectors().head)

    // Determine output object inspector (a struct of KEY, VALUE).
    val ois = new ArrayList[ObjectInspector]
    ois.add(keySer.getObjectInspector)
    ois.add(valueSer.getObjectInspector)

    val outputObjInspector = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(List("KEY","VALUE"), ois)
    }

    val joinTag = conf.getTag()

    // Propagate the output object inspector and serde infos to downstream operator.
    childOperators.foreach { child =>
      child match {
        case child: HiveTopOperator => {
          child.setInputObjectInspector(joinTag, outputObjInspector)
          child.setKeyValueTableDescs(joinTag,
              (conf.getKeySerializeInfo, conf.getValueSerializeInfo))
        }
        case _ => {
          throw new HiveException("%s's downstream operator should be %s. %s found.".format(
            this.getClass.getName, classOf[HiveTopOperator].getName, child.getClass.getName))
        }
      }
    }
  }

  /**
   * Initialize the object inspectors, evaluators, and serializers. Used on
   * both the master and the slave.
   */
  private def initializeOisAndSers(conf: ReduceSinkDesc, rowInspector: ObjectInspector) {
    keyEval = conf.getKeyCols.map(ExprNodeEvaluatorFactory.get(_)).toArray
    val numDistributionKeys = conf.getNumDistributionKeys()
    val distinctColIndices = conf.getDistinctColumnIndices()
    val numDistinctExprs = distinctColIndices.size()
    valueEval = conf.getValueCols.map(ExprNodeEvaluatorFactory.get(_)).toArray

    // Initialize serde for key columns.
    val keyTableDesc = conf.getKeySerializeInfo()
    keySer = keyTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    keySer.initialize(null, keyTableDesc.getProperties())
    val keyIsText = keySer.getSerializedClass().equals(classOf[Text])

    // Initialize serde for value columns.
    val valueTableDesc = conf.getValueSerializeInfo()
    valueSer = valueTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    valueSer.initialize(null, valueTableDesc.getProperties())

    // Initialize object inspector for key columns.
    keyObjInspector = SharkEnvSlave.objectInspectorLock.synchronized {
      ReduceSinkOperatorHelper.initEvaluatorsAndReturnStruct(
        keyEval,
        distinctColIndices,
        conf.getOutputKeyColumnNames,
        numDistributionKeys,
        rowInspector)
    }

    // Initialize object inspector for value columns.
    val valFieldInspectors = valueEval.map(eval => eval.initialize(rowInspector)).toList
    valObjInspector = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(
        conf.getOutputValueColumnNames(), valFieldInspectors)
    }

    // Initialize evaluator and object inspector for partition columns.
    partitionEval = conf.getPartitionCols.map(ExprNodeEvaluatorFactory.get(_)).toArray
    partitionObjInspectors = partitionEval.map(_.initialize(rowInspector)).toArray
  }

  /**
   * Process a partition when there is NO distinct key aggregations.
   */
  def processPartitionNoDistinct(iter: Iterator[_]) = {
    // Buffer for key, value evaluation to avoid frequent object allocation.
    val evaluatedKey = new Array[Object](keyEval.length)
    val evaluatedValue = new Array[Object](valueEval.length)

    // A random number generator, used to distribute keys evenly if there is
    // no partition columns specified. Use a constant seed to make the code
    // deterministic as did Hive.
    val rand = new Random(13)

    iter.map { row =>
      // TODO: we don't need partition code for group-by or join

      // Determine the partition code (Hive calls it keyHashCode), used for
      // DISTRIBUTED BY / CLUSTER BY.
      var partitionCode = 0
      if (partitionEval.size == 0) {
        // If there is no partition columns, we randomly distribute the data
        partitionCode = rand.nextInt()
      } else {
        // With partition columns, determine a partitionCode to distribute.
        var i = 0
        while (i < partitionEval.size) {
          val o = partitionEval(i).evaluate(row)
          partitionCode = partitionCode * 31 +
            ObjectInspectorUtils.hashCode(o, partitionObjInspectors(i))
          i += 1
        }
      }

      // Evaluate the key columns.
      var i = 0
      while (i < keyEval.length) {
        evaluatedKey(i) = keyEval(i).evaluate(row)
        i += 1
      }

      // Evaluate the value columns.
      i = 0
      while (i < valueEval.length) {
        evaluatedValue(i) = valueEval(i).evaluate(row)
        i += 1
      }

      val key = keySer.serialize(evaluatedKey, keyObjInspector).asInstanceOf[BytesWritable]
      val value = valueSer.serialize(evaluatedValue, valObjInspector).asInstanceOf[BytesWritable]
      val keyArr = new ReduceKey(new Array[Byte](key.getLength))
      keyArr.partitionCode = partitionCode
      val valueArr = new Array[Byte](value.getLength)

      // TODO(rxin): we don't need to copy bytes if we get rid of Spark block
      // manager's double buffering.
      Array.copy(key.getBytes, 0, keyArr.bytes, 0, key.getLength)
      Array.copy(value.getBytes, 0, valueArr, 0, value.getLength)
      (keyArr, valueArr)
    }
  }

  /**
   * Process a partition when there is distinct key aggregations. One row per
   * distinct column is emitted here.
   */
  def processPartitionDistinct(iter: Iterator[_]) = {
    // TODO(rxin): Rewrite this pile of code.

    // We output one row per distinct column
    val distinctExprs = conf.getDistinctColumnIndices()
    iter.flatMap { row =>
      val value = valueSer.serialize(valueEval.map(_.evaluate(row)), valObjInspector)
        .asInstanceOf[BytesWritable]

      val numDistributionKeys = conf.getNumDistributionKeys
      val allKeys = keyEval.map(_.evaluate(row)) // The key without distinct cols
      val currentKeys = new Array[Object](numDistributionKeys + 1)
      System.arraycopy(allKeys, 0, currentKeys, 0, numDistributionKeys)
      val serializedDistributionKey = keySer.serialize(currentKeys, keyObjInspector)
        .asInstanceOf[BytesWritable]
      val distributionKeyArray = new Array[Byte](serializedDistributionKey.getLength)
      Array.copy(serializedDistributionKey.getBytes, 0,
        distributionKeyArray, 0, serializedDistributionKey.getLength)
      val distributionKey = new ReduceKey(distributionKeyArray)
      for (i <- 0 until distinctExprs.size) yield {
        val distinctParameters = conf.getDistinctColumnIndices()(i).map(allKeys(_)).toArray
        currentKeys(numDistributionKeys) = new StandardUnion(i.toByte, distinctParameters)
        val key = keySer.serialize(currentKeys, keyObjInspector)
          .asInstanceOf[BytesWritable]
        val keyArr = new Array[Byte](key.getLength)
        val valueArr = new Array[Byte](value.getLength)
        Array.copy(key.getBytes, 0, keyArr, 0, key.getLength)
        Array.copy(value.getBytes, 0, valueArr, 0, value.getLength)
        (distributionKey, (keyArr, valueArr)).asInstanceOf[Any]
      }
    }
  }

}
