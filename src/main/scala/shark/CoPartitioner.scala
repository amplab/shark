package shark

import scala.collection.mutable.ArrayBuffer
import spark.HashPartitioner

class CoPartitioner(partitions: Int) extends HashPartitioner(partitions) {
  
  private val partCols = new ArrayBuffer[String]
  
  def addPartitionCols(col: String) {
    partCols += col
  }
  
  def getPartCols = partCols
}
