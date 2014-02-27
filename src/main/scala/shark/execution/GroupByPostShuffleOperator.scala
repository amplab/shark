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

package org.apache.hadoop.hive.ql.exec
// Put this file in Hive's exec package to access package level visible fields and methods.

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, HashSet => JHashSet, Set => JSet}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.plan.{ExprNodeColumnDesc, TableDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorUtils,
  StandardStructObjectInspector, StructObjectInspector, UnionObject}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.BytesWritable

import org.apache.spark.{Aggregator, HashPartitioner}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import shark.SharkEnv
import shark.execution._
import shark.execution.serialization.OperatorSerializationWrapper


// The final phase of group by.
// TODO(rxin): For multiple distinct aggregations, use sort-based shuffle.
class GroupByPostShuffleOperator extends GroupByPreShuffleOperator 
  with ReduceSinkTableDesc {

  @BeanProperty var keyTableDesc: TableDesc = _
  @BeanProperty var valueTableDesc: TableDesc = _

  // Use two sets of key deserializer and value deserializer because in sort-based aggregations,
  // we need to keep two rows deserialized at any given time (to compare whether we have seen
  // a new input group by key).
  @transient var keySer: Deserializer = _
  @transient var keySer1: Deserializer = _
  @transient var valueSer: Deserializer = _
  @transient var valueSer1: Deserializer = _

  @transient val distinctKeyAggrs = new JHashMap[Int, JSet[java.lang.Integer]]()
  @transient val nonDistinctKeyAggrs = new JHashMap[Int, JSet[java.lang.Integer]]()
  @transient val nonDistinctAggrs = new JArrayList[Int]()
  @transient val distinctKeyWrapperFactories = new JHashMap[Int, JArrayList[KeyWrapperFactory]]()
  @transient val distinctHashSets = new JHashMap[Int, JArrayList[JHashSet[KeyWrapper]]]()
  @transient var unionExprEvaluator: ExprNodeEvaluator = _

  override def createLocals() {
    super.createLocals()

    // Initialize unionExpr. KEY has union field as the last field if there are distinct aggrs.
    unionExprEvaluator = initializeUnionExprEvaluator(rowInspector)

    initializeKeyUnionAggregators()
    initializeKeyWrapperFactories()

    keySer = keyTableDesc.getDeserializerClass.newInstance()
    keySer.initialize(null, keyTableDesc.getProperties())
    keySer1 = keyTableDesc.getDeserializerClass.newInstance()
    keySer1.initialize(null, keyTableDesc.getProperties())

    valueSer = valueTableDesc.getDeserializerClass.newInstance()
    valueSer.initialize(null, valueTableDesc.getProperties())
    valueSer1 = valueTableDesc.getDeserializerClass.newInstance()
    valueSer1.initialize(null, valueTableDesc.getProperties())
  }
  
  override def createRemotes() {
    super.createRemotes()
    
    var kvd = keyValueDescs()
    keyTableDesc = kvd.head._2._1
    valueTableDesc = kvd.head._2._2
  }

  private def initializeKeyWrapperFactories() {
    distinctKeyAggrs.keySet.iterator.foreach { unionId =>
      val aggrIndices = distinctKeyAggrs.get(unionId)
      val evals = aggrIndices.map(i => aggregationParameterFields(i)).toArray
      val ois = aggrIndices.map(i => aggregationParameterObjectInspectors(i)).toArray
      val writableOis: Array[Array[ObjectInspector]] = ois.map { oi => oi.map { k =>
        ObjectInspectorUtils.getStandardObjectInspector(k, ObjectInspectorCopyOption.WRITABLE)
      }}.toArray

      val keys = new JArrayList[KeyWrapperFactory]()
      val hashSets = new JArrayList[JHashSet[KeyWrapper]]()
      for(i <- 0 until evals.size) {
        keys.add(new KeyWrapperFactory(evals(i), ois(i), writableOis(i)))
        hashSets.add(new JHashSet[KeyWrapper])
      }
      distinctHashSets.put(unionId, hashSets)
      distinctKeyWrapperFactories.put(unionId, keys)
    }
  }

  private def initializeUnionExprEvaluator(rowInspector: ObjectInspector): ExprNodeEvaluator = {
    val sfs = rowInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
    var unionExprEval: ExprNodeEvaluator = null
    if (sfs.size > 0) {
      val keyField = sfs.get(0)
      if (keyField.getFieldName.toUpperCase.equals(Utilities.ReduceField.KEY.name)) {
        val keyObjInspector = keyField.getFieldObjectInspector
        if (keyObjInspector.isInstanceOf[StandardStructObjectInspector]) {
          val keysfs = keyObjInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
          if (keysfs.size() > 0) {
            val sf = keysfs.get(keysfs.size() - 1)
            if (sf.getFieldObjectInspector().getCategory().equals(ObjectInspector.Category.UNION)) {
              unionExprEval = ExprNodeEvaluatorFactory.get(
                new ExprNodeColumnDesc(
                  TypeInfoUtils.getTypeInfoFromObjectInspector(sf.getFieldObjectInspector),
                  keyField.getFieldName + "." + sf.getFieldName, null, false
                )
              )
              unionExprEval.initialize(rowInspector)
            }
          }
        }
      }
    }
    unionExprEval
  }

  /**
   * This is used to initialize evaluators for distinct keys stored in
   * the union component of the key.
   */
  private def initializeKeyUnionAggregators() {
    val aggrs = conf.getAggregators
    for (i <- 0 until aggrs.size) {
      val aggr = aggrs.get(i)
      val parameters = aggr.getParameters
      for (j <- 0 until parameters.size) {
        if (unionExprEvaluator != null) {
          val names = parameters.get(j).getExprString().split("\\.")
          // parameters of the form : KEY.colx:t.coly
          if (Utilities.ReduceField.KEY.name().equals(names(0))) {
            val name = names(names.length - 2)
            val tag = Integer.parseInt(name.split("\\:")(1))
            if (aggr.getDistinct()) {
              var set = distinctKeyAggrs.get(tag)
              if (set == null) {
                set = new JHashSet[java.lang.Integer]()
                distinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            } else {
              var set = nonDistinctKeyAggrs.get(tag)
              if (set == null) {
                set = new JHashSet[java.lang.Integer]()
                nonDistinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            }
          } else {
            // will be VALUE._COLx
            if (!nonDistinctAggrs.contains(i)) {
              nonDistinctAggrs.add(i)
            }
          }
        }
      }
      if (parameters.size() == 0) {
        // for ex: count(*)
        if (!nonDistinctAggrs.contains(i)) {
          nonDistinctAggrs.add(i)
        }
      }
    }
  }

  override def execute(): RDD[_] = {
    val inputRdd = executeParents().head._2.asInstanceOf[RDD[(Any, Any)]]

    var numReduceTasks = hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS)
    // If we have no keys, it needs a total aggregation with 1 reducer.
    if (numReduceTasks < 1 || conf.getKeys.size == 0) numReduceTasks = 1
    val partitioner = new ReduceKeyPartitioner(numReduceTasks)

    val repartitionedRDD = new ShuffledRDD[Any, Any, (Any, Any)](inputRdd, partitioner)
      .setSerializer(SharkEnv.shuffleSerializerName)

    if (distinctKeyAggrs.size > 0) {
      // If there are distinct aggregations, do sort-based aggregation.
      val op = OperatorSerializationWrapper(this)

      repartitionedRDD.mapPartitions(iter => {
        // Sort the input based on the key.
        val buf = iter.toArray.asInstanceOf[Array[(ReduceKeyReduceSide, Array[Byte])]]
        val sorted = buf.sortWith((x, y) => x._1.compareTo(y._1) < 0).iterator

        // Perform sort-based aggregation.
        op.initializeOnSlave()
        op.sortAggregate(sorted)
      }, preservesPartitioning = true)

    } else {
      // No distinct keys.
      val aggregator = new Aggregator[Any, Any, ArrayBuffer[Any]](
        GroupByAggregator.createCombiner _, GroupByAggregator.mergeValue _, GroupByAggregator.mergeCombiners _)
      val hashedRdd = repartitionedRDD.mapPartitionsWithContext(
        (context, iter) => aggregator.combineValuesByKey(iter, context),
        preservesPartitioning = true)

      val op = OperatorSerializationWrapper(this)
      hashedRdd.mapPartitionsWithIndex { case(split, partition) =>
        op.initializeOnSlave()
        op.hashAggregate(partition)
      }
    }
  }

  def sortAggregate(iter: Iterator[_]) = {
    logDebug("Running Post Shuffle Group-By")

    if (iter.hasNext) {
      // Sort based aggregation iterator.
      new Iterator[Array[Object]]() {

        private var currentKeySer = keySer
        private var currentValueSer = valueSer
        private var nextKeySer = keySer1
        private var nextValueSer = valueSer1

        private val outputBuffer = new Array[Object](keyFields.length + aggregationEvals.length)
        private val bytes = new BytesWritable()
        private val row = new Array[Object](2)
        private val nextRow = new Array[Object](2)
        private val aggrs = newAggregations()

        private val newKeys: KeyWrapper = keyFactory.getKeyWrapper()

        private var _hasNext: Boolean = fetchNextInputTuple()
        private val currentKeys: KeyWrapper = newKeys.copyKey()

        override def hasNext = _hasNext

        private def swapSerDes() {
          var tmp = currentKeySer
          currentKeySer = nextKeySer
          nextKeySer = tmp
          tmp = currentValueSer
          currentValueSer = nextValueSer
          nextValueSer = tmp
        }

        /**
         * Fetch the next input tuple and deserialize them, store the keys in newKeys.
         * @return True if successfully fetched; false if we have reached the end.
         */
        def fetchNextInputTuple(): Boolean = {
          if (!iter.hasNext) {
            false
          } else {
            val (key: ReduceKeyReduceSide, value: Array[Byte]) = iter.next()
            bytes.set(key.byteArray, 0, key.length)
            nextRow(0) = nextKeySer.deserialize(bytes)
            bytes.set(value, 0, value.length)
            nextRow(1) = nextValueSer.deserialize(bytes)
            newKeys.getNewKey(nextRow, rowInspector)
            swapSerDes()
            true
          }
        }

        override def next(): Array[Object] = {

          currentKeys.copyKey(newKeys)
          resetAggregations(aggrs)

          // Use to track whether we have moved to a new distinct column.
          var currentUnionTag = -1
          // Use to track whether we have seen a new distinct value for the current distinct column.
          var lastDistinctKey: Array[Object] = null

          // Keep processing inputs until we see a new group by key.
          // In that case, we need to emit a new output tuple so the next() call should end.
          while (_hasNext && newKeys.equals(currentKeys)) {
            row(0) = nextRow(0)
            row(1) = nextRow(1)

            assert(unionExprEvaluator != null)
            // union tag is the tag (index) for the distinct column.
            val uo = unionExprEvaluator.evaluate(row).asInstanceOf[UnionObject]
            val unionTag = uo.getTag.toInt

            if (unionTag == 0) {
              // Aggregate the non distinct columns from the values.
              for (pos <- nonDistinctAggrs) {
                val o = new Array[Object](aggregationParameterFields(pos).length)
                var pi = 0
                while (pi < aggregationParameterFields(pos).length) {
                  o(pi) = aggregationParameterFields(pos)(pi).evaluate(row)
                  pi += 1
                }
                aggregationEvals(pos).aggregate(aggrs(pos), o)
              }
            }

            // update non-distinct aggregations : "KEY._colx:t._coly"
            val nonDistinctKeyAgg: JSet[java.lang.Integer] = nonDistinctKeyAggrs.get(unionTag)
            if (nonDistinctKeyAgg != null) {
              for (pos <- nonDistinctKeyAgg) {
                val o = new Array[Object](aggregationParameterFields(pos).length)
                var pi = 0
                while (pi < aggregationParameterFields(pos).length) {
                  o(pi) = aggregationParameterFields(pos)(pi).evaluate(row)
                  pi += 1
                }
                aggregationEvals(pos).aggregate(aggrs(pos), o)
              }
            }

            // update distinct aggregations
            val distinctKeyAgg: JSet[java.lang.Integer] = distinctKeyAggrs.get(unionTag)
            if (distinctKeyAgg != null) {
              if (currentUnionTag != unionTag) {
                // New union tag, i.e. move to a new distinct column.
                currentUnionTag = unionTag
                lastDistinctKey = null
              }

              val distinctKeyAggIter = distinctKeyAgg.iterator
              while (distinctKeyAggIter.hasNext) {
                val pos = distinctKeyAggIter.next()
                val o = new Array[Object](aggregationParameterFields(pos).length)
                var pi = 0
                while (pi < aggregationParameterFields(pos).length) {
                  o(pi) = aggregationParameterFields(pos)(pi).evaluate(row)
                  pi += 1
                }

                if (lastDistinctKey == null) {
                  lastDistinctKey = new Array[Object](o.length)
                }

                val isNewDistinct = ObjectInspectorUtils.compare(
                  o,
                  aggregationParameterObjectInspectors(pos),
                  lastDistinctKey,
                  aggregationParameterStandardObjectInspectors(pos)) != 0

                if (isNewDistinct) {
                  // This is a new distinct value for the column.
                  aggregationEvals(pos).aggregate(aggrs(pos), o)
                  var pi = 0
                  while (pi < o.length) {
                    lastDistinctKey(pi) = ObjectInspectorUtils.copyToStandardObject(
                      o(pi), aggregationParameterObjectInspectors(pos)(pi),
                      ObjectInspectorCopyOption.WRITABLE)
                    pi += 1
                  }
                }
              }
            }

            // Finished processing the current input tuple. Check if there are more inputs.
            _hasNext = fetchNextInputTuple()
          } // end of while (_hasNext && newKeys.equals(currentKeys))

          // Copy output keys and values to our reused output cache
          var i = 0
          val numKeys = keyFields.length
          while (i < numKeys) {
            outputBuffer(i) = keyFields(i).evaluate(row)
            i += 1
          }
          while (i < numKeys + aggrs.length) {
            outputBuffer(i) = aggregationEvals(i - numKeys).evaluate(aggrs(i - numKeys))
            i += 1
          }
          outputBuffer
        } // End of def next(): Array[Object]
      } // End of Iterator[Array[Object]]
    } else {
      // The input iterator is empty.
      if (keyFields.length == 0) Iterator(createEmptyRow()) else Iterator.empty
    }
  }

  def hashAggregate(iter: Iterator[_]) = {
    // TODO: use MutableBytesWritable to avoid the array copy.
    val bytes = new BytesWritable()
    logDebug("Running Post Shuffle Group-By")
    val outputCache = new Array[Object](keyFields.length + aggregationEvals.length)

    // The reusedRow is used to conform to Hive's expected row format.
    // It is an array of [key, value] that is reused across rows
    val reusedRow = new Array[Any](2)
    val aggrs = newAggregations()

    val newIter = iter.map { case (key: ReduceKeyReduceSide, values: Seq[Array[Byte]]) =>
      bytes.set(key.byteArray, 0, key.length)
      val deserializedKey = keySer.deserialize(bytes)
      reusedRow(0) = deserializedKey
      resetAggregations(aggrs)
      values.foreach { case v: Array[Byte] =>
        bytes.set(v, 0, v.length)
        reusedRow(1) = valueSer.deserialize(bytes)
        aggregateExistingKey(reusedRow, aggrs)
      }

      // Copy output keys and values to our reused output cache
      var i = 0
      val numKeys = keyFields.length
      while (i < numKeys) {
        outputCache(i) = keyFields(i).evaluate(reusedRow)
        i += 1
      }
      while (i < numKeys + aggrs.length) {
        outputCache(i) = aggregationEvals(i - numKeys).evaluate(aggrs(i - numKeys))
        i += 1
      }
      outputCache
    }

    if (!newIter.hasNext && keyFields.length == 0) {
      Iterator(createEmptyRow()) // We return null if there are no rows
    } else {
      if (groupingSetsPresent) {
        // When the cardinality of grouping sets is more than
        // hive.new.job.grouping.set.cardinality, HIVE will generate a 2-stage MR plan
        // for the GroupBy clause and grouping sets are handled in the reduce-side GroupBy
        // operator of the first stage. In this case, no distinct keys are allowed. We handle
        // this case here.
        val outputBuffer = new Array[Array[Object]](groupingSets.size)
        newIter.flatMap { row: Array[Object] =>
          val newKeysIter = getNewKeysIterator(row)

          var i = 0
          while (newKeysIter.hasNext) {
            newKeysIter.next
            outputBuffer(i) = row.clone()
            i += 1
          }
          outputBuffer
        }
      } else {
        newIter
      }
    }
  }

  private def createEmptyRow(): Array[Object] = {
    val aggrs = newAggregations()
    val output = new Array[Object](aggrs.length)
    var i = 0
    while (i < aggrs.length) {
      var emptyObj: Array[Object] = null
      if (aggregationParameterFields(i).length > 0) {
        emptyObj = aggregationParameterFields.map { field => null }.toArray
      }
      aggregationEvals(i).aggregate(aggrs(i), emptyObj)
      output(i) = aggregationEvals(i).evaluate(aggrs(i))
      i += 1
    }
    output
  }

  @inline
  private def resetAggregations(aggs: Array[AggregationBuffer]) {
    var i = 0
    while (i < aggs.length) {
      aggregationEvals(i).reset(aggs(i))
      i += 1
    }
  }
}


object GroupByAggregator {
  def createCombiner(v: Any) = ArrayBuffer(v)
  def mergeValue(buf: ArrayBuffer[Any], v: Any) = buf += v
  def mergeCombiners(c1: ArrayBuffer[Any], c2: ArrayBuffer[Any]) = c1 ++ c2
}
