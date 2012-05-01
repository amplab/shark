package shark.exec

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
import spark.RDD
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

  override def preprocessRdd[T](rdd: RDD[T]): RDD[_] = {
    // TODO: hasOrder and limit should really be made by optimizer.
    val hasOrder = parentOperator match {
      case op: ReduceSinkOperator => op.getConf.getOrder != null
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
          RDDUtils.sortLeastKByKey(rdd.asInstanceOf[RDD[(ReduceKey, Any)]], l)
        }
        case None => processOrderedRDD(rdd)
      }
    } else {
      rdd
    }
  }

  override def processPartition[T](iter: Iterator[T]) = {
    val bytes = new BytesWritable()
    iter map { row =>
      row match {
        case (key, value: Array[Byte]) => {
          bytes.set(value)
          valueDeser.deserialize(bytes)
        }
        case other => throw new Exception("error with: " + other)
      }
    }
  }

  def processOrderedRDD[K <% Ordered[K]: ClassManifest, V: ClassManifest, T](rdd: RDD[T]): RDD[_] = {
    rdd match {
      case r: RDD[(K, V)] => r.sortByKey()
      case _ => rdd
    }
  }
}

