package shark

import scala.collection.mutable.ArrayBuffer
import spark.HashPartitioner

class CoPartitioner(partitions: Int) extends HashPartitioner(partitions) {
  
  private val partCols = new ArrayBuffer[String]
  
  def addPartitionCol(col: String) {
    partCols += col
  }
  
  def getPartCols = partCols
  
  override def equals(other: Any): Boolean = {
    other match {
      case other: HashPartitioner =>
        numPartitions == other.numPartitions
      case _ => false
    }
  }
}
