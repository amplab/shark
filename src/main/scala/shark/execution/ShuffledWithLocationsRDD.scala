package spark.rdd

import spark.{Partition, Partitioner, RDD}

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param parent the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 */
class ShuffledWithLocationsRDD[K, V](
    @transient parent: RDD[(K, V)],
    part: Partitioner,
    locations: Array[Seq[String]]) extends ShuffledRDD(parent, part) {

  override def getPreferredLocations(split: Partition) = 
    if (locations != null) 
      locations(split.asInstanceOf[ShuffledRDDPartition].index)
    else 
      Nil
}
