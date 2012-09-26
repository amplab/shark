package shark.execution

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{ExtractOperator => HiveExtractOperator}
import org.apache.hadoop.hive.ql.plan.{ExtractDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.io.BytesWritable

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.RDDUtils
import spark.{RDD, Split}
import spark.SparkContext._


class ExtractOperator extends UnaryOperator[HiveExtractOperator]
with HiveTopOperator {

  @BeanProperty var conf: ExtractDesc = _
  @BeanProperty var valueTableDesc: TableDesc = _
  @BeanProperty var localHconf: HiveConf = _

  @transient var eval: ExprNodeEvaluator = _
  @transient var valueDeser: Deserializer = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
    localHconf = super.hconf
    valueTableDesc = keyValueTableDescs.values.head._2
  }

  override def initializeOnSlave() {
    eval = ExprNodeEvaluatorFactory.get(conf.getCol)
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

    val isTotalOrder = parentOperator match {
      case op: ReduceSinkOperator => op.getConf.getNumReducers == 1
      case _ => false
    }

    val limit =
      if (childOperators.size == 1) {
        childOperators.head match {
          case op: LimitOperator => Some(op.limit)
          case _ => None
        }
      } else {
        None
      }

    if (hasOrder) {
      limit match {
        case Some(l) => {
          logInfo("Pushing limit (%d) down to sorting".format(l))
          if (isTotalOrder) {
            logInfo("Performing Order By Limit")
            RDDUtils.sortLeastKByKey(rdd.asInstanceOf[RDD[(ReduceKey, Any)]], l)
          } else {
            // We always do a distribute by. I'm not sure if we need to if there are no distribution keys.
            val distributedRdd = rdd.asInstanceOf[RDD[(ReduceKey, Any)]].partitionBy(new ReduceKeyPartitioner(rdd.splits.size))
            logInfo("Performing Sort By Limit")
            RDDUtils.partialSortLeastKByKey(distributedRdd, l)
          }
        }
        case None => {
          if (isTotalOrder) {
            logInfo("Performing Order By")
            processOrderedRDD(rdd)
          } else {
            // We always do a distribute by. I'm not sure if we need to if there are no distribution keys.
            logInfo("Performing Distribute By Sort By")
            val clusteredRdd = rdd.asInstanceOf[RDD[(ReduceKey, Any)]].partitionBy(new ReduceKeyPartitioner(rdd.splits.size))
            clusteredRdd.mapPartitions { partition => partition.toSeq.sortWith(_._1 < _._1).iterator }
            // Not sure if toSeq is better than toArray
          }
        }
      }
    } else {
      if (isTotalOrder) {
        logInfo("Partitioning data to a single reducer")
        rdd.asInstanceOf[RDD[(ReduceKey, Any)]].partitionBy(new ReduceKeyPartitioner(1))
      } else {
        logInfo("Performing Distribute By")
        rdd.asInstanceOf[RDD[(ReduceKey, Any)]].partitionBy(new ReduceKeyPartitioner(rdd.splits.size))
      }
    }
  }

  override def processPartition(split: Split, iter: Iterator[_]) = {
    val bytes = new BytesWritable()
    iter map {
      case (key, value: Array[Byte]) => {
        bytes.set(value)
        valueDeser.deserialize(bytes)
      }
    }
  }

  def processOrderedRDD[K <% Ordered[K]: ClassManifest, V: ClassManifest, T](rdd: RDD[_]): RDD[_] = {
    rdd match {
      case r: RDD[(K, V)] => r.sortByKey()
      case _ => rdd
    }
  }
}

