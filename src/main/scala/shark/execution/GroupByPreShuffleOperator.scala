package org.apache.hadoop.hive.ql.exec
// Put this file in Hive's exec package to access package level visible fields and methods.

import java.util.{ArrayList, HashMap => JHashMap}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.plan.{AggregationDesc, ExprNodeDesc, ExprNodeColumnDesc, GroupByDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
    ObjectInspectorUtils, StandardStructObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.SharkEnvSlave
import shark.execution.UnaryOperator
import spark.RDD
import spark.SparkContext._


/**
 * The pre-shuffle group by operator responsible for map side aggregations.
 */
class GroupByPreShuffleOperator extends UnaryOperator[HiveGroupByOperator] {

  @BeanProperty var conf: GroupByDesc = _
  @BeanProperty var minReductionHashAggr: Float = _
  @BeanProperty var numRowsCompareHashAggr: Int = _

  @transient var keyFactory: KeyWrapperFactory = _
  @transient var rowInspector: ObjectInspector = _

  // The aggregation functions.
  @transient var aggregationEvals: Array[GenericUDAFEvaluator] = _

  // Key fields to be grouped.
  @transient var keyFields: Array[ExprNodeEvaluator] = _
  // A struct object inspector composing of all the fields.
  @transient var keyObjectInspector: StructObjectInspector = _

  @transient var aggregationParameterFields: Array[Array[ExprNodeEvaluator]] = _
  @transient var aggregationParameterObjectInspectors: Array[Array[ObjectInspector]] = _
  @transient var aggregationIsDistinct: Array[Boolean] = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
    minReductionHashAggr = hconf.get(HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION.varname).toFloat
    numRowsCompareHashAggr = hconf.get(HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL.varname).toInt
  }

  override def initializeOnSlave() {
    aggregationEvals = conf.getAggregators.map(_.getGenericUDAFEvaluator).toArray
    aggregationIsDistinct = conf.getAggregators.map(_.getDistinct).toArray
    rowInspector = objectInspector.asInstanceOf[StructObjectInspector]
    keyFields = conf.getKeys().map(k => ExprNodeEvaluatorFactory.get(k)).toArray
    val keyObjectInspectors: Array[ObjectInspector] = keyFields.map(k => k.initialize(rowInspector))
    val currentKeyObjectInspectors = SharkEnvSlave.objectInspectorLock.synchronized {
      keyObjectInspectors.map { k =>
        ObjectInspectorUtils.getStandardObjectInspector(k, ObjectInspectorCopyOption.WRITABLE)
      }
    }

    aggregationParameterFields = conf.getAggregators.toArray.map { aggr =>
      aggr.asInstanceOf[AggregationDesc].getParameters.toArray.map { param =>
        ExprNodeEvaluatorFactory.get(param.asInstanceOf[ExprNodeDesc])
      }
    }
    aggregationParameterObjectInspectors = aggregationParameterFields.map { aggr =>
      aggr.map { param => param.initialize(rowInspector) }
    }

    val aggObjInspectors = aggregationEvals.zipWithIndex.map { pair =>
      pair._1.init(conf.getAggregators.get(pair._2).getMode,
        aggregationParameterObjectInspectors(pair._2))
    }

    val keyFieldNames = conf.getOutputColumnNames.slice(0, keyFields.length)
    val totalFields = keyFields.length + aggregationEvals.length
    val keyois = new ArrayList[ObjectInspector](totalFields)
    keyObjectInspectors.foreach(keyois.add(_))

    keyObjectInspector = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(keyFieldNames, keyois)
    }

    keyFactory = new KeyWrapperFactory(keyFields, keyObjectInspectors, currentKeyObjectInspectors)
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    logInfo("Running Pre-Shuffle Group-By")
    var numRowsInput = 0
    var numRowsHashTbl = 0
    var useHashAggr = true

    // Do aggregation on map side using hashAggregations hash table.
    val hashAggregations = new JHashMap[KeyWrapper, Array[AggregationBuffer]]()

    val newKeys: KeyWrapper = keyFactory.getKeyWrapper()

    while (iter.hasNext && useHashAggr) {
      val row = iter.next().asInstanceOf[AnyRef]
      numRowsInput += 1

      newKeys.getNewKey(row, rowInspector)
      newKeys.setHashKey()

      var aggs = hashAggregations.get(newKeys)
      var isNewKey = false
      if (aggs == null) {
        isNewKey = true
        val newKeyProber = newKeys.copyKey()
        aggs = newAggregations()
        hashAggregations.put(newKeyProber, aggs)
        numRowsHashTbl += 1
      }
      aggregate(row, aggs, isNewKey)

      // Disable partial hash-based aggregation if desired minimum reduction is
      // not observed after initial interval.
      if (numRowsInput == numRowsCompareHashAggr) {
        if (numRowsHashTbl > numRowsInput * minReductionHashAggr) {
          useHashAggr = false
          logInfo("Mapside hash aggregation disabled")
        } else {
          logInfo("Mapside hash aggregation enabled")
        }
        logInfo("#hash table="+numRowsHashTbl+" #rows="+
          numRowsInput+" reduction="+numRowsHashTbl.toFloat/numRowsInput+
          " minReduction="+minReductionHashAggr)
      }
    }

    // Generate an iterator for the aggregation output from hashAggregations.
    val outputCache = new Array[Object](keyFields.length + aggregationEvals.length)
    hashAggregations.toIterator.map { case(key, aggrs) =>
      val keyArr = key.getKeyArray()
      var i = 0
      while (i < keyArr.length) {
        outputCache(i) = keyArr(i)
        i += 1
      }
      i = 0
      while (i < aggrs.length) {
        outputCache(i + keyArr.length) = aggregationEvals(i).evaluate(aggrs(i))
        i += 1
      }
      outputCache
    } ++
    // Concatenate with iterator for remaining rows not in hashAggregations.
    iter.map { case row: AnyRef =>
      newKeys.getNewKey(row, rowInspector)
      val newAggrKey = newKeys.copyKey()
      val aggrs = newAggregations()
      aggregate(row, aggrs, true)
      val keyArr = newAggrKey.getKeyArray()
      var i = 0
      while (i < keyArr.length) {
        outputCache(i) = keyArr(i)
        i += 1
      }
      i = 0
      while (i < aggrs.length) {
        outputCache(i + keyArr.length) = aggregationEvals(i).evaluate(aggrs(i))
        i += 1
      }
      outputCache
    }
  }

  protected def aggregate(row: AnyRef, aggregations: Array[AggregationBuffer], isNewKey: Boolean) {
    var i = 0
    while (i < aggregations.length) {
      if (!aggregationIsDistinct(i) || isNewKey) {
        aggregationEvals(i).aggregate(
          aggregations(i), aggregationParameterFields(i).map(_.evaluate(row)))
      }
      i += 1
    }
  }

  protected def newAggregations(): Array[AggregationBuffer] = {
    aggregationEvals.map(eval => eval.getNewAggregationBuffer)
  }
}
