package shark.exec

import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, ObjectInspectorUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject
import org.apache.hadoop.hive.serde2.{Deserializer, SerDe}
import org.apache.hadoop.io.BytesWritable

import java.util.HashMap
import java.util.HashSet
import java.util.Set
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.KeyWrapperFactory
import spark.RDD
import spark.SparkContext._


class GroupByPostShuffleOperator extends GroupByPreShuffleOperator 
with HiveTopOperator {

  @BeanProperty var keyTableDesc: TableDesc = _
  @BeanProperty var valueTableDesc: TableDesc = _
  
  @transient var keySer: Deserializer = _
  @transient var valueSer: Deserializer = _
  @transient val distinctKeyAggrs = new HashMap[Int, Set[java.lang.Integer]]()
  @transient val nonDistinctKeyAggrs = new HashMap[Int, Set[java.lang.Integer]]()
  @transient val nonDistinctAggrs = new ArrayList[Int]()
  @transient val distinctKeyWrapperFactories = new HashMap[Int, ArrayList[KeyWrapperFactory]]()
  @transient val distinctHashSets = new HashMap[Int, ArrayList[HashSet[KeyWrapperFactory#ListKeyWrapper]]]()

  override def initializeOnMaster() {
    super.initializeOnMaster()
    keyTableDesc = keyValueTableDescs.values.head._1
    valueTableDesc = keyValueTableDescs.values.head._2
  }
  
  override def initializeOnSlave() {
    
    super.initializeOnSlave()
    initializeKeyUnionAggregators()
    initializeKeyWrapperFactories()

    keySer = keyTableDesc.getDeserializerClass.newInstance().asInstanceOf[Deserializer]
    keySer.initialize(null, keyTableDesc.getProperties())

    valueSer = valueTableDesc.getDeserializerClass.newInstance().asInstanceOf[SerDe]
    valueSer.initialize(null, valueTableDesc.getProperties())
  }
  
  def initializeKeyWrapperFactories() {
    
    distinctKeyAggrs.keySet.iterator.foreach { unionId => {
      val aggrIndices = distinctKeyAggrs.get(unionId)
      val evals = aggrIndices.map( i => aggregationParameterFields(i) ).toArray
      val ois = aggrIndices.map( i => aggregationParameterObjectInspectors(i) ).toArray
      val writableOis = ois.map( oi => oi.map( k => ObjectInspectorUtils.getStandardObjectInspector(k, ObjectInspectorCopyOption.WRITABLE) )).toArray

      val keys = new ArrayList[KeyWrapperFactory]()      
      val hashSets = new ArrayList[HashSet[KeyWrapperFactory#ListKeyWrapper]]()
      for(i <- 0 until evals.size) {
        keys.add(new KeyWrapperFactory(evals(i), ois(i), writableOis(i)))
        hashSets.add(new HashSet[KeyWrapperFactory#ListKeyWrapper])
      } 
      distinctHashSets.put(unionId, hashSets)
      distinctKeyWrapperFactories.put(unionId, keys)
    }}

  }

  /**
   * This is used to initialize evaluators for distinct keys stored in
   * the union component of the key.
   */
  def initializeKeyUnionAggregators() {
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
                set = new HashSet[java.lang.Integer]()
                distinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            } else {
              var set = nonDistinctKeyAggrs.get(tag)
              if (set == null) {
                set = new HashSet[java.lang.Integer]()
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

  override def preprocessRdd[T](rdd: RDD[T]): RDD[_] = {
    var numReduceTasks = -1
    try {
      numReduceTasks = java.lang.Integer.parseInt(hconf.get("mapred.reduce.tasks").trim)
    } catch {
      case e: Exception => println("Invalid mapred.reduce.tasks: " + hconf.get("mapred.reduce.tasks").trim)
    }
    if (numReduceTasks < 1 || conf.getKeys.size == 0) numReduceTasks = 1 // Hive conf defaults to -1
    rdd.asInstanceOf[RDD[(Any, Any)]].groupByKey(numReduceTasks)
  }
  
  override def processPartition[T](iter: Iterator[T]) = {
    //TODO: we should support outputs besides BytesWritable in case a different SerDe is used
    val bytes = new BytesWritable()
    logInfo("Running Post Shuffle Group-By")
    val outputCache = new Array[Object](keyFields.length + aggregationEvals.length)
    val keys = keyFactory.getKeyWrapper()
    val newIter = iter.map(pair => {
      pair match {
        case (key: ReduceKey,
              values: Seq[_]) => {
          
          bytes.set(key.bytes)
          val deserializedKey = deserializeKey(bytes)
          val aggrs = newAggregations()
          values.foreach(v => {
            v match {
              case v: Array[Byte] => {
                bytes.set(v)
                val deserializedValue = deserializeValue(bytes)
                keys match {
                  case k: KeyWrapperFactory#ListKeyWrapper => 
                    k.getNewKey(Array(deserializedKey,deserializedValue), rowInspector)
                  case k: KeyWrapperFactory#TextKeyWrapper => 
                    k.getNewKey(Array(deserializedKey,deserializedValue), rowInspector)
                }
                aggregate(Array(deserializedKey, deserializedValue), aggrs, false)
              }
              case (key: Array[Byte], value: Array[Byte]) => {
                bytes.set(key)
                val deserializedUnionKey = deserializeKey(bytes)
                bytes.set(value)
                val deserializedValue = deserializeValue(bytes)
                val row = Array(deserializedUnionKey, deserializedValue)

                keys match {
                  case k: KeyWrapperFactory#ListKeyWrapper => 
                    k.getNewKey(row, rowInspector)
                  case k: KeyWrapperFactory#TextKeyWrapper => 
                    k.getNewKey(row, rowInspector)
                }
                val uo =  unionExprEvaluator.evaluate(row).asInstanceOf[UnionObject]
                val unionTag = uo.getTag().toInt
                // Handle non-distincts in the key-union
                if (nonDistinctKeyAggrs.get(unionTag) != null) {
                  nonDistinctKeyAggrs.get(unionTag).foreach { i => {
                    val o = aggregationParameterFields(i).map(_.evaluate(row)).toArray
                    aggregationEvals(i).aggregate(aggrs(i), o)
                  }}
                }
                // Handle non-distincts in the value
                if (unionTag == 0) {
                  nonDistinctAggrs.foreach { i => {
                    val o = aggregationParameterFields(i).map(_.evaluate(row)).toArray
                    aggregationEvals(i).aggregate(aggrs(i), o)
                  }}
                }
                // Handle distincts
                if (distinctKeyAggrs.get(unionTag) != null) {
                  // This assumes that we traverse the aggr Params in the same order
                  val aggrIndices = distinctKeyAggrs.get(unionTag).iterator
                  val factories = distinctKeyWrapperFactories.get(unionTag)
                  val hashes = distinctHashSets.get(unionTag)
                  for (i <- 0 until factories.size) {
                    val aggrIndex = aggrIndices.next
                    // We currently only do ListKeyWrapper
                    val key = factories.get(i).getKeyWrapper().asInstanceOf[KeyWrapperFactory#ListKeyWrapper]
                    key.getNewKey(row, rowInspector)
                    key.setHashKey()
                    var seen = hashes(i).contains(key)
                    if (!seen) {
                      aggregationEvals(aggrIndex).aggregate(aggrs(aggrIndex), key.getKeyArray)
                      hashes(i).add(key.copyKey().asInstanceOf[KeyWrapperFactory#ListKeyWrapper])
                    }
                  }
                }
              }
            }
          })
          val arr = keys match {
            case k: KeyWrapperFactory#ListKeyWrapper => 
              k.getKeyArray
            case k: KeyWrapperFactory#TextKeyWrapper => 
              k.getKeyArray
          }
          // Reset hash sets for next group-by key
          distinctHashSets.values.foreach { hashes =>
            for (i <- 0 until hashes.size) {
              hashes(i).clear()
            }
          }
          arr.zipWithIndex foreach { case(key, i) => outputCache(i) = key }
          aggrs.zipWithIndex.foreach { case(aggr, i) => 
            outputCache(i + arr.length) = aggregationEvals(i).evaluate(aggr)
          }
          
          outputCache
        }
      }
    })
    if (!newIter.hasNext && keyFields.size == 0) {
      Iterator(createEmptyRow()) // We return null if there are no rows
    } else {
      newIter
    }
  }

  def createEmptyRow(): Array[Object] = {
    val aggrs = newAggregations()
    val output = new Array[Object](aggrs.size)
    for (i <- 0 until aggrs.size) {
      var emptyObj: Array[Object] = null
      if (aggregationParameterFields(i).length > 0) {
        emptyObj = aggregationParameterFields.map { field => null }.toArray
      }
      aggregationEvals(i).aggregate(aggrs(i), emptyObj)
      output(i) = aggregationEvals(i).evaluate(aggrs(i))
    }
    output
  }
  
  def deserializeKey(bytes: BytesWritable) = {    
    keySer.asInstanceOf[Deserializer].deserialize(bytes)
  }
  
  def deserializeValue(bytes: BytesWritable) = {
    valueSer.asInstanceOf[Deserializer].deserialize(bytes)
  }

}

