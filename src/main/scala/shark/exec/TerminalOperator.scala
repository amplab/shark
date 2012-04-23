package shark.exec

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator, JobCloseFeedBack}
import org.apache.hadoop.hive.serde2.Serializer

import scala.reflect.BeanProperty

import shark.{CacheKey, RDDUtils, SharkEnv}
import spark.RDD


/**
 * File sink operator. Most of the work is delegated to the Sink class. It
 * can accomplish one of the three things:
 * - write query output to disk
 * - cache query output
 * - return query as RDD directly (without materializing it)
 */
class TerminalOperator extends TerminalAbstractOperator[HiveFileSinkOperator] with Serializable {

  @BeanProperty var sink: Sink = _

  // Create a local copy of hconf and hiveSinkOp so we can XML serialize it.
  @BeanProperty var localHiveOp: HiveFileSinkOperator = _
  @BeanProperty var localHconf: HiveConf = _

  override def initializeOnMaster() {
    localHconf = super.hconf
    // Set parent to null so we won't serialize the entire query plan.
    hiveOp.setParentOperators(null)
    hiveOp.setChildOperators(null)
    hiveOp.setInputObjInspectors(null)
    localHiveOp = hiveOp
  }

  override def initializeOnSlave() {
    sink.sinkOp = this
    localHiveOp.initialize(localHconf, Array(objectInspector))
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = sink.processPartition(iter)

  override def postprocessRdd[T](rdd: RDD[T]): RDD[_] = {
    val returnRdd = sink.postProcessRDD(rdd)
    hiveOp.jobClose(localHconf, true, new JobCloseFeedBack)
    returnRdd
  }

  def useFileSink() { sink = new FileSink }

  def useCacheSink(ctasTableName: String) { sink = new CacheSink(ctasTableName) }
  
  def useTableRddSink() { sink = new TableRddSink }

}


/**
 * Sink defines behavior of the sink operations. By default, it doesn't do
 * anything. One of the use*Sink method must be called before executing the
 * query plan. 
 */
abstract class Sink extends Serializable {
  @transient var sinkOp: TerminalOperator = _
  def processPartition[T](iter: Iterator[T]): Iterator[_] = iter
  def postProcessRDD[T](rdd: RDD[T]): RDD[_] = rdd
}


/**
 * Writes the final RDD to file system. We avoid using anonymous class here
 * for easier debugging ...
 */
class CacheSink(@BeanProperty var tableName: String) extends Sink with Serializable {
  
  def this() = this(null)
 
  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    RDDUtils.serialize(
        iter,
        sinkOp.localHconf,
        sinkOp.localHiveOp.getConf.getTableInfo,
        sinkOp.objectInspector)
  }

  override def postProcessRDD[T](rdd: RDD[T]): RDD[_] = {
    SharkEnv.cache.put(new CacheKey(tableName), rdd)
    rdd.foreach(_ => Unit)
    rdd
  }
}


/**
 * Cache the RDD and force evaluate it (so the cache is filled). We avoid
 * using anonymous class here for easier debugging ...
 */
class FileSink extends Sink with Serializable {
  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    iter.foreach { row =>
      sinkOp.localHiveOp.processOp(row, 0)
    }
    sinkOp.localHiveOp.closeOp(false)
    iter
  }

  override def postProcessRDD[T](rdd: RDD[T]): RDD[_] = {
    rdd.foreach(_ => Unit)
    rdd
  }
}


/**
 * Collect the output as a TableRDD.
 */
class TableRddSink extends Sink with Serializable {}

