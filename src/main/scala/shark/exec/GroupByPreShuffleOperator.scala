package shark.exec

import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.plan.{AggregationDesc, ExprNodeDesc, ExprNodeColumnDesc, GroupByDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, ObjectInspectorUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.{StandardStructObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.{KeyWrapperFactory, SharkEnv}
import spark.RDD
import spark.SparkContext._


class GroupByPreShuffleOperator extends UnaryOperator[HiveGroupByOperator] {
  
  @BeanProperty var conf: GroupByDesc = _

  @transient var keyFactory: KeyWrapperFactory = _
  @transient var rowInspector: ObjectInspector = _
  @transient var aggregationEvals: Array[GenericUDAFEvaluator] = _
  @transient var keyObjectInspector: StructObjectInspector = _
  @transient var unionExprEvaluator: ExprNodeEvaluator = _

  @transient var keyFields: Array[ExprNodeEvaluator] = _
  @transient var keyObjectInspectors: Array[ObjectInspector] = _
  @transient var aggregationParameterFields: Array[Array[ExprNodeEvaluator]] = _
  @transient var aggregationParameterObjectInspectors: Array[Array[ObjectInspector]] = _
  @transient var aggregationIsDistinct: Array[Boolean] = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }

  override def initializeOnSlave() {
    aggregationEvals = conf.getAggregators.map { agg =>
      (agg.getGenericUDAFEvaluator.getClass).newInstance.asInstanceOf[GenericUDAFEvaluator]
    }.toArray
    
    aggregationIsDistinct = conf.getAggregators.map { agg => agg.getDistinct }.toArray
    rowInspector = objectInspector.asInstanceOf[StructObjectInspector]
    keyFields = conf.getKeys().map(k => ExprNodeEvaluatorFactory.get(k)).toArray
    keyObjectInspectors = keyFields.map(k => k.initialize(rowInspector))
    val currentKeyObjectInspectors = SharkEnv.objectInspectorLock.synchronized {
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

    val fieldNames = conf.getOutputColumnNames    
    val keyFieldNames = fieldNames.slice(0,keyFields.length)
    
    val totalFields = keyFields.length + aggregationEvals.length
    val keyois = new ArrayList[ObjectInspector](totalFields)
    keyObjectInspectors.foreach(keyois.add(_))

    keyObjectInspector = SharkEnv.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(
        keyFieldNames, keyois)
    }
    // Initialize unionExpr. KEY has union field as the last field if there are distinct aggrs.
    unionExprEvaluator = initializeUnionExprEvaluator(rowInspector)
    
    keyFactory = new KeyWrapperFactory(keyFields, keyObjectInspectors, currentKeyObjectInspectors)
  }
  
  def initializeUnionExprEvaluator(rowInspector: ObjectInspector): ExprNodeEvaluator = {
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

  override def processPartition[T](iter: Iterator[T]) = {
    logInfo("Pre-Shuffle Group-By")
    val hashAggregations = new java.util.HashMap[KeyWrapperFactory#KeyWrapper, Array[AggregationBuffer]]()
    val newKeys = keyFactory.getKeyWrapper()
    iter.foreach { case row: AnyRef =>
      
      newKeys match {
        case k: KeyWrapperFactory#ListKeyWrapper => {
          k.getNewKey(row, rowInspector)
          k.setHashKey()
        }
        case k: KeyWrapperFactory#TextKeyWrapper => {
          k.getNewKey(row, rowInspector)
          k.setHashKey()
        }
      }
      var aggs = hashAggregations.get(newKeys)
      var newKey = false
      if (aggs == null) {
        newKey = true
        val newKeyProber = newKeys match {
          case k: KeyWrapperFactory#ListKeyWrapper => {
            k.copyKey()
          }
          case k: KeyWrapperFactory#TextKeyWrapper => {
            k.copyKey()
          }
        }
        aggs = newAggregations()
        hashAggregations.put(newKeyProber, aggs)
      }
      aggregate(row, aggs, newKey)
      Unit
    }
    
    val outputCache = new Array[Object](keyFields.length + aggregationEvals.length)
    hashAggregations.toIterator.map { case(key, aggrs) =>
      val arr = key match {
        case k: KeyWrapperFactory#ListKeyWrapper => 
          k.getKeyArray
        case k: KeyWrapperFactory#TextKeyWrapper => 
          k.getKeyArray
      }
                                     
      arr.zipWithIndex foreach { case(key, i) => outputCache(i) = key }
      aggrs.zipWithIndex foreach { case(aggr, i) => outputCache(i + arr.length) = aggregationEvals(i).evaluate(aggr) }
      outputCache
    }
  }

  def aggregate(row: AnyRef, aggregations: Array[AggregationBuffer], isNewKey: Boolean) {
    var i = 0
    while (i < aggregations.length) {
      if (!aggregationIsDistinct(i) || isNewKey) {
        //TODO: remove .map and reuse aggregationParameterFields array
        aggregationEvals(i).aggregate(aggregations(i), 
                                      aggregationParameterFields(i).map(_.evaluate(row)))
      }
      i += 1
    }
  }
  
  def newAggregations(): Array[AggregationBuffer] = {
    aggregationEvals.map(eval => eval.getNewAggregationBuffer)
  }

  def getAggregationParameterFields() = aggregationParameterFields
  
  def getAggregationEvals: Array[GenericUDAFEvaluator] = aggregationEvals

  def getKeyFields: Array[ExprNodeEvaluator] = keyFields

  def getKeyObjectInspector: ObjectInspector = keyObjectInspector

}
