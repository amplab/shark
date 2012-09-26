package shark.memstore

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

  override def hashCode(): Int = key.hashCode()
}
