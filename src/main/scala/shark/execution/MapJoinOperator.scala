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

import java.util.{ArrayList, HashMap => JHashMap, List => JList}

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.NullWritable

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, JoinUtil => HiveJoinUtil}
import org.apache.hadoop.hive.ql.plan.MapJoinDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory

import org.apache.spark.rdd.RDD

import shark.SharkEnv
import shark.execution.serialization.{OperatorSerializationWrapper, SerializableWritable}


/**
 * A join operator optimized for joining a large table with a number of small
 * tables that fit in memory. The join can be performed as a map only job that
 * avoids an expensive shuffle process.
 *
 * Different from Hive, we don't spill the hash tables to disk. If the "small"
 * tables are too big to fit in memory, the normal join should be used anyway.
 */
class MapJoinOperator extends CommonJoinOperator[MapJoinDesc] {

  @BeanProperty var posBigTable: Int = _
  @BeanProperty var bigTableAlias: Int = _
  @BeanProperty var bigTableAliasByte: java.lang.Byte = _

  @transient var joinKeys: Array[JList[ExprNodeEvaluator]] = _
  @transient var joinKeysObjectInspectors: Array[JList[ObjectInspector]] = _

  @transient val metadataKeyTag = -1
  @transient var joinValues: Array[JList[ExprNodeEvaluator]] = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    posBigTable = conf.getPosBigTable()
    bigTableAlias = order(posBigTable).toInt
    bigTableAliasByte = bigTableAlias.toByte

    // Also call initialize on slave since we want the joinKeys and joinVals to
    // be initialized so we can use them in combineMultipleRdds(). This also puts
    // serialization info for keys in MapJoinMetaData.
    initializeOnSlave()
    initializeJoinFilterOnMaster()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    tagLen = conf.getTagLength()
    joinKeys = new Array[JList[ExprNodeEvaluator]](tagLen)
    HiveJoinUtil.populateJoinKeyValue(
      joinKeys, conf.getKeys(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    // A bit confusing but getObjectInspectorsFromEvaluators also initializes
    // the evaluators.
    joinKeysObjectInspectors = HiveJoinUtil.getObjectInspectorsFromEvaluators(
      joinKeys, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE, tagLen)

  }
  
  // copied from the org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator
  override def outputObjectInspector() = {
    var outputObjInspector = super.outputObjectInspector()
    val structFields = outputObjInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
    if (conf.getOutputColumnNames().size() < structFields.size()) {
      val structFieldObjectInspectors = new ArrayList[ObjectInspector]
      for (alias <- order) {
        val sz = conf.getExprs().get(alias).size()
        val retained = conf.getRetainList().get(alias)
        for (i <- 0 to sz - 1) {
          val pos = retained.get(i)
          structFieldObjectInspectors.add(structFields.get(pos).getFieldObjectInspector())
        }
      }
      outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        conf.getOutputColumnNames(),
        structFieldObjectInspectors)
    }
    
    outputObjInspector
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def executeParents(): Seq[(Int, RDD[_])] = {
    order.zip(parentOperators).map(x => (x._1.toInt, x._2.execute()))
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    logDebug("%d small tables to map join a large table (%d)".format(rdds.size - 1, posBigTable))
    logDebug("Big table alias " + bigTableAlias)

    val op1 = OperatorSerializationWrapper(this)

    initializeOnSlave()

    // Build hash tables for the small tables.
    val hashtables = rdds.zipWithIndex.filter(_._2 != bigTableAlias).map { case ((_, rdd), pos) =>

      logDebug("Creating hash table for input %d".format(pos))

      // First compute the keys and values of the small RDDs on slaves.
      // We need to do this before collecting the RDD because the RDD might
      // contain lazy structs that cannot be properly collected directly.
      val posByte = pos.toByte


      // Create a local reference for the serialized arrays, otherwise the
      // following mapParititons will fail because it tries to include the
      // outer closure, which references "this".
      val op = op1
      // An RDD of (Join key, Corresponding rows) tuples.
      val rddForHash: RDD[(Seq[AnyRef], Seq[Array[AnyRef]])] =
        rdd.mapPartitions { partition =>
          op.initializeOnSlave()
          // Put serialization metadata for values in slave's MapJoinMetaData.
          // Needed to serialize values in collect().
          //op.setValueMetaData(posByte)
          op.computeJoinKeyValuesOnPartition(partition, posByte)
        }

      // Collect the RDD and build a hash table.
      val startCollect = System.currentTimeMillis()
      val collectedRows: Array[(Seq[AnyRef], Seq[Array[AnyRef]])] = rddForHash.collect()

      logDebug("collectedRows size:" + collectedRows.size)
      val collectTime = System.currentTimeMillis() - startCollect
      logInfo("HashTable collect took " + collectTime + " ms")

      // Build the hash table.
      val hash = collectedRows.groupBy(x => x._1)
       .mapValues(v => v.flatMap(t => t._2))

      val map = new JHashMap[Seq[AnyRef], Array[Array[AnyRef]]]()
      hash.foreach(x => map.put(x._1, x._2))
      (pos, map)
    }.toMap

    val fetcher = SharkEnv.sc.broadcast(hashtables)
    val op = op1
    rdds(bigTableAlias)._2.mapPartitions { partition =>
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave()
      val newPart = op.joinOnPartition(partition, fetcher.value)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      newPart
    }
  }

  def computeJoinKeyValuesOnPartition[T](iter: Iterator[T], posByte: Byte)
  : Iterator[(Seq[AnyRef], Seq[Array[AnyRef]])] = {
    // MapJoinObjectValue contains a MapJoinRowContainer, which contains a list of
    // rows to be joined.
    val valueMap = new JHashMap[Seq[AnyRef], Seq[Array[AnyRef]]]
    iter.foreach { row =>
      val key = JoinUtil.computeJoinKey(
        row,
        joinKeys(posByte),
        joinKeysObjectInspectors(posByte))
      val value: Array[AnyRef] = JoinUtil.computeJoinValues(
        row,
        joinVals(posByte),
        joinValuesObjectInspectors(posByte),
        joinFilters(posByte),
        joinFilterObjectInspectors(posByte),
        filterMap == null,
        serializable = true)
      // If we've seen the key before, just add it to the row container wrapped by
      // corresponding MapJoinObjectValue.
      val objValue = valueMap.get(key)
      if (objValue == null) {
        valueMap.put(key, Seq[Array[AnyRef]](value))
      } else {
        valueMap.put(key, objValue ++ List[Array[AnyRef]](value))
      }
    }
    valueMap.iterator
  }

  /**
   * Stream through the large table and process the join using the hash tables.
   * Note that this is a specialized processPartition that accepts an extra
   * parameter for the hash tables (built from the small tables).
   */
  def joinOnPartition[T](iter: Iterator[T],
      hashtables: Map[Int, JHashMap[Seq[AnyRef], Array[Array[AnyRef]]]]): Iterator[_] = {

    val joinKeyEval = joinKeys(bigTableAlias)
    val joinValueEval = joinVals(bigTableAlias)
    val bufs = new Array[Seq[Array[Object]]](numTables)
    val nullSafes = conf.getNullSafes()

    val cp = new CartesianProduct[Array[Object]](numTables)

    val jointRows: Iterator[Array[Array[Object]]] = iter.flatMap { row =>
      // Build the join key and value for the row in the large table.
      val key = JoinUtil.computeJoinKey(
        row,
        joinKeyEval,
        joinKeysObjectInspectors(bigTableAlias))
      val value: Array[AnyRef] = JoinUtil.computeJoinValues(
        row,
        joinValueEval,
        joinValuesObjectInspectors(bigTableAlias),
        joinFilters(bigTableAlias),
        joinFilterObjectInspectors(bigTableAlias),
        filterMap == null)

      if (nullCheck && JoinUtil.joinKeyHasAnyNulls(key, nullSafes)) {
        val bufsNull = Array.fill[Seq[Array[Object]]](numTables)(Seq())
        bufsNull(bigTableAlias) = Seq(value)
        cp.product(bufsNull, joinConditions)
      } else {
        // Build the join bufs.
        var i = 0
        while ( i < numTables) {
          if (i == bigTableAlias) {
            bufs(i) = Seq[Array[AnyRef]](value)
          } else {
            val smallTableValues = hashtables.getOrElse(i, null).getOrElse(key, null)
            bufs(i) =
              if (smallTableValues == null) {
                Seq.empty[Array[AnyRef]]
              } else {
                smallTableValues.map { x =>
                  x.map(v => v.asInstanceOf[SerializableWritable[_]].value.asInstanceOf[AnyRef])
                }
              }
          }
          i += 1
        }
        cp.product(bufs, joinConditions)
      }
    }

    jointRows.map(elems => generate(elems))
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
    throw new UnsupportedOperationException("MapJoinOperator.processPartition()")
  }
}
