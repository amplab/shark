package spark.rdd

import spark.{Partition, Partitioner, RDD}
import shark.SharkEnv

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param parent the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @param locations the locations of each partition
 * @tparam K the key class.
 * @tparam V the value class.
 */
class ShuffledWithLocationsRDD[K, V](
    @transient parent: RDD[(K, V)],
    part: Partitioner,
    @transient locations: Array[Seq[String]]) 
    extends ShuffledRDD(parent, part, SharkEnv.shuffleSerializerName) {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (locations != null) 
      locations(split.asInstanceOf[ShuffledRDDPartition].index)
    else 
      Nil
  }
}
