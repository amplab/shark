package shark.memstore

import spark.RDD
import spark.storage.StorageLevel

class CacheManager {

  val keyToRdd = new collection.mutable.HashMap[CacheKey, RDD[_]]()

  val keyToStats = new collection.mutable.HashMap[CacheKey, collection.Map[Int, TableStats]]

  def put(key: CacheKey, rdd: RDD[_], storageLevel: StorageLevel) {
    keyToRdd(key) = rdd
    rdd.persist(storageLevel)
  }

  def get(key: CacheKey): Option[RDD[_]] = keyToRdd.get(key)

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Seq[String] = {
    keyToRdd.keys.map(_.key).collect { case k: String => k } toSeq
  }

}
