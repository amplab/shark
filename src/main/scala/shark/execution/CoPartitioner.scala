package shark

import scala.collection.mutable.ArrayBuffer
import spark.HashPartitioner

class CoPartitioner(partitions: Int) extends HashPartitioner(partitions) {
  
  private val _partCols = new ArrayBuffer[String]
  
  def addPartitionCol(col: String) {
    _partCols += col
  }
  
  def partitionColumns = _partCols
  
  override def equals(other: Any): Boolean = {
    other match {
      case other: HashPartitioner =>
        numPartitions == other.numPartitions
      case _ => false
    }
  }
}
