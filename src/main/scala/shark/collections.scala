package shark

import scala.collection.TraversableLike
import scala.collection.mutable.{ Builder }

object collections {

  object Conversions {

    implicit def keyValuePairSeq[K, V](seq: Seq[(K, V)]): KeyValuePairSeq[K, V] =
      new KeyValuePairSeq(seq)

  }


  class KeyValuePairSeq[K, V](seq: Seq[(K, V)]) {
  
    protected[this] def newBuilder: Builder[V, Seq[V]] = Seq.newBuilder[V]

    def groupByKey() = {
      val m = scala.collection.mutable.Map.empty[K, Builder[V, Seq[V]]]
      for (elem <- seq) {
        val bldr = m.getOrElseUpdate(elem._1, newBuilder)
        bldr += elem._2
      }
      val b = scala.collection.immutable.Map.newBuilder[K, Seq[V]]
      for ((k, v) <- m)
        b += ((k, v.result))
    
      b.result
    }

  }

}


