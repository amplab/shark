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

import java.util.{ArrayList, Arrays, List => JList, Random}

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion
import org.apache.hadoop.io.BytesWritable


/**
 * Converts a collection of rows into key, value pairs. This is the
 * upstream operator for joins and groupbys.
 */
class ReduceSinkOperator extends UnaryOperator[ReduceSinkDesc] {

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
  @transient var keyFieldObjInspectors: Array[ObjectInspector] = _
  @transient var valObjInspector: ObjectInspector = _
  @transient var valFieldObjInspectors: Array[ObjectInspector] = _
  @transient var partitionObjInspectors: Array[ObjectInspector] = _

  override def getTag() = conf.getTag()

  override def initializeOnMaster() {
    super.initializeOnMaster()
    
    conf = desc
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    
    initializeOisAndSers(conf, objectInspector)
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    if (conf.getDistinctColumnIndices().size() == 0) {
      processPartitionNoDistinct(iter)
    } else {
      processPartitionDistinct(iter)
    }
  }

  override def outputObjectInspector() = {
    initializeOisAndSers(conf, objectInspector)
    
    val ois = new ArrayList[ObjectInspector]
    ois.add(keySer.getObjectInspector)
    ois.add(valueSer.getObjectInspector)
    ObjectInspectorFactory.getStandardStructObjectInspector(List("KEY", "VALUE"), ois)
  }

  // will be used of the children operators (in JoinOperator/Extractor/GroupByPostShuffleOperator
  def getKeyValueTableDescs() = (conf.getKeySerializeInfo, conf.getValueSerializeInfo)
  
  /**
   * Initialize the object inspectors, evaluators, and serializers. Used on
   * both the master and the slave.
   */
  private def initializeOisAndSers(conf: ReduceSinkDesc, rowInspector: ObjectInspector) {
    keyEval = conf.getKeyCols.map(ExprNodeEvaluatorFactory.get(_)).toArray
    keyFieldObjInspectors = initEvaluators(keyEval, 0, keyEval.length, rowInspector)
    
    val numDistributionKeys = conf.getNumDistributionKeys()
    val distinctColIndices = conf.getDistinctColumnIndices()
    valueEval = conf.getValueCols.map(ExprNodeEvaluatorFactory.get(_)).toArray

    // Initialize serde for key columns.
    val keyTableDesc = conf.getKeySerializeInfo()
    keySer = keyTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    keySer.initialize(null, keyTableDesc.getProperties())

    // Initialize serde for value columns.
    val valueTableDesc = conf.getValueSerializeInfo()
    valueSer = valueTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    valueSer.initialize(null, valueTableDesc.getProperties())

    // Initialize object inspector for key columns.
    keyObjInspector = initEvaluatorsAndReturnStruct(
      keyEval,
      distinctColIndices,
      conf.getOutputKeyColumnNames,
      numDistributionKeys,
      rowInspector)

    // Initialize object inspector for value columns.
    valFieldObjInspectors = valueEval.map(eval => eval.initialize(rowInspector))
    valObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      conf.getOutputValueColumnNames(), valFieldObjInspectors.toList)

    // Initialize evaluator and object inspector for partition columns.
    partitionEval = conf.getPartitionCols.map(ExprNodeEvaluatorFactory.get(_)).toArray
    partitionObjInspectors = partitionEval.map(_.initialize(rowInspector)).toArray
  }

  /**
   * Process a partition when there is NO distinct key aggregations.
   */
  def processPartitionNoDistinct(iter: Iterator[_]) = {
    val numDistributionKeys = conf.getNumDistributionKeys
    // Buffer for key, value evaluation to avoid frequent object allocation.
    val evaluatedKey = new Array[Object](keyEval.length)
    val evaluatedValue = new Array[Object](valueEval.length)
    val reduceKey = new ReduceKeyMapSide

    // A random number generator, used to distribute keys evenly if there is
    // no partition columns specified. Use a constant seed to make the code
    // deterministic as did Hive.
    val rand = new Random(13)

    iter.map { row =>
      // TODO: we don't need partition code for group-by or join

      // Determine the partition code (Hive calls it keyHashCode), used for
      // DISTRIBUTED BY / CLUSTER BY.
      var partitionCode = 0
      if (partitionEval.length == 0) {
        // If there is no partition columns, we randomly distribute the data
        partitionCode = rand.nextInt()
      } else {
        // With partition columns, determine a partitionCode to distribute.
        var i = 0
        while (i < partitionEval.length) {
          val o = partitionEval(i).evaluate(row)
          partitionCode = partitionCode * 31 +
            ObjectInspectorUtils.hashCode(o, partitionObjInspectors(i))
          i += 1
        }
      }

      // Evaluate the key columns.
      var i = 0
      while (i < numDistributionKeys) {
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

      reduceKey.bytesWritable = key
      reduceKey.partitionCode = partitionCode
      (reduceKey, value)
    }
  }

  /**
   * Process a partition when there is distinct key aggregations. One row per
   * distinct column is emitted here.
   */
  def processPartitionDistinct(iter: Iterator[_]) = {
    // The following is copied from an email from Cliff Engle:
    //
    // First, I want to clarify that the UnionObjectInspector has nothing to do with a SQL union.
    // It's just an unfortunate naming coincidence.
    //
    // What a union does is allows a field to have one of multiple types. For instance, if you had
    // a UnionObjectInspector that unioned {boolean, int, string}, its field could be only one of
    // either of those three. The UnionObjectInspector identifies which of the possible types that
    // field is based on a byte tag  that is stored before that actual data, e.g. a field could be
    // like {0:true}, {1:4}, or {2:"abc"} for the previous example.
    //
    // In terms of its usage, I believe that the UnionObjectInspector is only used for group-by's
    // with more than one distinct, e.g.
    //   select c, sum(distinct a), count(distinct b) from table group by c;
    //
    // (Note that a and b can be different types). The purpose of the tag is so that
    // multiple-distincts can occur in a single shuffle. There is an example in this link:
    // https://issues.apache.org/jira/browse/HIVE-537, but I'll walk through this one too.
    //
    // First, we must send all records with the same group-by key (aka distribution key here) to
    // the same reducer to get a complete grouping. Next, the shuffle phase needs to sort the data
    // such that the reducer can stream through the rows and produce both distinct aggregates
    // for a given group-by key. This means that rows must be sorted by the distribution key,
    // along with their respective distinct aggregation key. The UnionObjectInspector is what
    // allows the reducer to sort by each distinct key independently in a single reduce.
    //
    // Here's an example with some trivial data:
    //  table (a: int, b: string, c: int)
    //  1, "x", 1
    //  4, "z", 2
    //  2, "x", 1
    //  3, "y", 1
    //
    // When sent to the ReduceSink, each row produces two output rows (because of the two distincts)
    // whose keys look like this:
    //  (1, {0:1})
    //  (1, {1:"x"})
    //  (2, {0:4})
    //  (2, {1:"z"})
    //  (1, {0:2})
    //  (1, {1:"x"})
    //  (1, {0:3})
    //  (1, {1:"y"})
    //
    // Note that the squiggles represent {tag:field} for the Union object. After the data is sorted
    // by this key, the reducer can perform the distinct aggregations using fixed size memory since
    // distinct values will always be read sequentially by the reducer.

    val numDistributionKeys = conf.getNumDistributionKeys
    val numValues = valueEval.length
    val valueBuffer = new Array[Object](numValues)

    // keyBuffer holds the group by keys plus an extra distinct column.
    val allKeys = new Array[Object](keyEval.length)
    val keyBuffer = new Array[Object](numDistributionKeys + 1)

    // Populate the partition code of the reduce key using hash code of the group by keys.
    val reduceKey = new ReduceKeyMapSide

    // We output one row per distinct column
    val distinctColumnIndices: Array[Array[Int]] = {
      conf.getDistinctColumnIndices().map { indices: JList[java.lang.Integer] =>
        indices.map(_.intValue()).toArray
      }.toArray
    }

    val distinctBuffer = Array.tabulate[Array[Object]](distinctColumnIndices.length) { i =>
      new Array[Object](distinctColumnIndices(i).length)
    }

    val emptyValue = new BytesWritable

    iter.flatMap { row =>
      // Evaluate the values and serialize them into a BytesWritable.
      var i = 0
      while (i < numValues) {
        valueBuffer(i) = valueEval(i).evaluate(row)
        i += 1
      }
      val value = valueSer.serialize(valueBuffer, valObjInspector).asInstanceOf[BytesWritable]

      // Evaluate the distribution key. This is the group by key (i.e. no distinct columns).
      i = 0
      while (i < keyEval.length) {
        allKeys(i) = keyEval(i).evaluate(row)
        i += 1
      }
      System.arraycopy(allKeys, 0, keyBuffer, 0, numDistributionKeys)
      keyBuffer(numDistributionKeys) = null

      // The partition code determines where we are sending the rows.
      reduceKey.partitionCode = Arrays.hashCode(keyBuffer)

      Iterator.tabulate(distinctColumnIndices.length) { distinctI =>
        var i = 0
        val distinctParameters: Array[Object] = distinctBuffer(distinctI)
        while (i < distinctColumnIndices(distinctI).length) {
          val aggPos = distinctColumnIndices(distinctI)(i)
          distinctParameters(i) = allKeys(aggPos)
          i += 1
        }

        keyBuffer(numDistributionKeys) = new StandardUnion(distinctI.toByte, distinctParameters)
        reduceKey.bytesWritable = keySer.serialize(
          keyBuffer, keyObjInspector).asInstanceOf[BytesWritable]

        // Only send the value if this is the first distinct expression.
        // Type is (ReduceKeyMapSide, BytesWritable)
        if (distinctI == 0) {
          (reduceKey, value)
        } else {
          (reduceKey, emptyValue)
        }
      }
    }
  }

}
