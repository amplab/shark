package shark

import spark.RDD
import scala.collection.mutable.HashMap


case class CacheKey[T](val key: T) {
  override def equals(o: Any): Boolean = {
    o match {
      case ck: CacheKey[_] => ck.key match {
        case k: T => k == key
        case _ => false
      }
      case _ => false
    }
  }
  
  override def hashCode(): Int = {
    key.hashCode()
  }
}


class CacheManager {
  val keyToRdd = new HashMap[CacheKey[_], RDD[_]]()

  def put(key: CacheKey[_], rdd: RDD[_]) {
    keyToRdd(key) = rdd
    rdd.cache()
  }

  def get(key: CacheKey[_]): Option[RDD[_]] = {
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

