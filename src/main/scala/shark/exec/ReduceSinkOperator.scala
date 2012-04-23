package shark.exec

import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.{ReduceSinkOperator => HiveReduceSinkOperator}
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.{ReduceSinkDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, SerDe}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, ObjectInspectorUtils}
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion
import org.apache.hadoop.io.{BytesWritable, Text}

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.operators.ReduceSinkWrapper


/**
 * Converts a collection of rows into key, value pairs. This is usually the
 * upstream operator for joins and groupbys.
 */
class ReduceSinkOperator extends UnaryOperator[HiveReduceSinkOperator] with Serializable {

  @BeanProperty var conf: ReduceSinkDesc = _

  @transient var keySer: SerDe = _
  @transient var valueSer: SerDe = _
  @transient var keyObjInspector: ObjectInspector = _
  @transient var valObjInspector: ObjectInspector = _
  @transient var partitionObjInspectors: Array[ObjectInspector] = _
  @transient var keyEval: Array[ExprNodeEvaluator] = _
  @transient var valueEval: Array[ExprNodeEvaluator] = _
  @transient var partitionEval: Array[ExprNodeEvaluator] = _

  override def getTag() = conf.getTag()

  def initializeDownStreamHiveOperator() {

    conf = hiveOp.getConf()

    // Note that we get input object inspector from hiveOp rather than Shark's
    // objectInspector because initializeMasterOnAll() hasn't been invoked yet.
    val vars = ReduceSinkOperator.initializeOisAndSers(conf, hiveOp.getInputObjInspectors().head)
    keyEval = vars._1
    valueEval = vars._2
    keySer = vars._3
    valueSer = vars._4
    keyObjInspector = vars._5
    valObjInspector = vars._6

    // Determine output object inspector (a struct of KEY, VALUE).
    val ois = new ArrayList[ObjectInspector]
    ois.add(keySer.getObjectInspector)
    ois.add(valueSer.getObjectInspector)
    val outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        List("KEY","VALUE"), ois)

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

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }

  override def initializeOnSlave() {
    val vars = ReduceSinkOperator.initializeOisAndSers(conf, objectInspector)
    keyEval = vars._1
    valueEval = vars._2
    keySer = vars._3
    valueSer = vars._4
    keyObjInspector = vars._5
    valObjInspector = vars._6
    partitionEval = vars._7
    partitionObjInspectors = vars._8
  }

  override def processPartition[T](iter: Iterator[T]) = {
    if (conf.getDistinctColumnIndices().size() == 0) { 
      // Normal case with no distinct columns
      iter.map { row =>
        // Note: we currently only support BinarySerDe's (not Text)
        val key = keySer.serialize(keyEval.map(_.evaluate(row)), keyObjInspector)
          .asInstanceOf[BytesWritable]
        val value = valueSer.serialize(valueEval.map(_.evaluate(row)), valObjInspector)
          .asInstanceOf[BytesWritable]
        val keyArr = new ReduceKey(new Array[Byte](key.getLength))
        val valueArr = new Array[Byte](value.getLength)
        Array.copy(key.getBytes, 0, keyArr.bytes, 0, key.getLength)
        Array.copy(value.getBytes, 0, valueArr, 0, value.getLength)
        (keyArr, valueArr)
      }
    } else {
      // We output one row per distinct column
      val distinctExprs = conf.getDistinctColumnIndices()
      iter.flatMap {  row =>
        val value = valueSer.serialize(valueEval.map(_.evaluate(row)), valObjInspector)
          .asInstanceOf[BytesWritable]

        var partitionCode = 0
        for (i <- 0 until partitionEval.size) {
          val o = partitionEval(i).evaluate(row)
          partitionCode = partitionCode * 31 +
            ObjectInspectorUtils.hashCode(o, partitionObjInspectors(i))
        }
        
        val numDistributionKeys = conf.getNumDistributionKeys
        val allKeys = keyEval.map(_.evaluate(row)) // The key without distinct cols
        val currentKeys = new Array[Object](numDistributionKeys + 1)
        System.arraycopy(allKeys, 0, currentKeys, 0, numDistributionKeys)
        val serializedDistributionKey = keySer.serialize(currentKeys, keyObjInspector).asInstanceOf[BytesWritable]
        val distributionKeyArray = new Array[Byte](serializedDistributionKey.getLength)
        Array.copy(serializedDistributionKey.getBytes, 0, distributionKeyArray, 0, serializedDistributionKey.getLength)
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
}


object ReduceSinkOperator {

  def initializeOisAndSers(conf: ReduceSinkDesc, rowInspector: ObjectInspector) = {
    val keyEval = conf.getKeyCols.map(ExprNodeEvaluatorFactory.get(_)).toArray
    val numDistributionKeys = conf.getNumDistributionKeys()
    val distinctColIndices = conf.getDistinctColumnIndices()
    val numDistinctExprs = distinctColIndices.size()
    val valueEval = conf.getValueCols.map(ExprNodeEvaluatorFactory.get(_)).toArray

    val keyTableDesc = conf.getKeySerializeInfo()
    val keySer = keyTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    keySer.initialize(null, keyTableDesc.getProperties())
    val keyIsText = keySer.getSerializedClass().equals(classOf[Text])

    val valueTableDesc = conf.getValueSerializeInfo()
    val valueSer = valueTableDesc.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    valueSer.initialize(null, valueTableDesc.getProperties())
    
    val keyObjInspector = ReduceSinkWrapper.initEvaluatorsAndReturnStruct(
      keyEval,
      distinctColIndices,
      conf.getOutputKeyColumnNames,
      numDistributionKeys,
      rowInspector)

    val valFieldInspectors = valueEval.map(eval => eval.initialize(rowInspector)).toList
    val valObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      conf.getOutputValueColumnNames(), valFieldInspectors)
      

    val partitionEval = conf.getPartitionCols.map(ExprNodeEvaluatorFactory.get(_)).toArray
    val partitionObjInspectors = partitionEval.map(_.initialize(rowInspector)).toArray
    (keyEval, valueEval, keySer, valueSer, keyObjInspector, valObjInspector, partitionEval, partitionObjInspectors)
  }

}

