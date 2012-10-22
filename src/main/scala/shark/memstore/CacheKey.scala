package shark.memstore

case class CacheKey(keyStr: String) {

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