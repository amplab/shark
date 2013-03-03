package shark

import collection.JavaConverters._

import java.io.EOFException
import java.nio.ByteBuffer
import java.util.NoSuchElementException

import shark.memstore2._

import spark.{Dependency, RDD, SerializableWritable, SparkContext, Partition, TaskContext}

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private class TachyonTablePartition(rddId: Int, idx: Int, val locations: Seq[String])
  extends Partition {

  override val index: Int = idx
}

/**
 * An RDD that reads a Tachyon Table.
 */
class TachyonTableRDD(
    sc: SparkContext,
    tablePath: String
    )
  extends RDD[ColumnarStruct](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val rawTable = SharkEnv.tachyonClient.getRawTable(tablePath)
    val rawColumn = rawTable.getRawColumn(0)
    val partitions = rawColumn.getPartitions()
    val array = new Array[Partition](partitions)
    for (i <- 0 until partitions) {
      val hosts = rawColumn.getPartition(i).getLocationHosts().asScala
      array(i) = new TachyonTablePartition(id, i, hosts)
    }
    array
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val rawTable = SharkEnvSlave.tachyonClient.getRawTable(tablePath)
    val buffers: Array[ByteBuffer] = new Array[ByteBuffer](rawTable.getColumns())
    for (i <- 0 until rawTable.getColumns()) {
      logInfo("Getting Column " + i + " partition " + 0 + " from Tachyon")
      val rawColumn = rawTable.getRawColumn(i)
      val file = rawColumn.getPartition(0)
      file.open("r")
      buffers(i) = file.readByteBuffer()
    }
    val partition = new TablePartition(buffers)
    partition.iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    // TODO: Filtering out "localhost" in case of file:// URLs
    val tachyonSplit = split.asInstanceOf[TachyonTablePartition]
    tachyonSplit.locations.filter(_ != "localhost")
  }

  // override def checkpoint() {
  //   // Do nothing. Hadoop RDD should not be checkpointed.
  // }
}
