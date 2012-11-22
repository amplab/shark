package shark

import spark.RDD
import spark.storage.StorageLevel
import scala.collection.mutable.HashMap


case class CacheKey(val keyStr: String) {
  val key: String = keyStr.toLowerCase()

  override def equals(o: Any): Boolean = {
    o match {
      case ck: CacheKey => key.equals(ck.key)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    key.hashCode()
  }
}


class CacheManager {
  val keyToRdd = new HashMap[CacheKey, RDD[_]]()

  def put(key: CacheKey, rdd: RDD[_]) {
    keyToRdd(key) = rdd
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def get(key: CacheKey): Option[RDD[_]] = {
    keyToRdd.get(key) match {
      case Some(rdd) => Some(RDDUtils.deserialize(rdd))
      case None => None
    }
  }

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Seq[String] = {
    keyToRdd.keys.map { _.key } collect { case k: String => k} toSeq
  }

}

