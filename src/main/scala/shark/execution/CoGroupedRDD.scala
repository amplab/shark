package spark

import java.net.URL
import java.io.EOFException
import java.io.ObjectInputStream
import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

// A version of CoGroupedRDD with the following changes:
// - Disable map-side aggregation.
// - Enforce return type to Array[ArrayBuffer].

sealed trait CoGroupSplitDep extends Serializable
case class NarrowCoGroupSplitDep(rdd: RDD[_], split: Split) extends CoGroupSplitDep
case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep


class CoGroupSplit(idx: Int, val deps: Seq[CoGroupSplitDep]) extends Split with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

// Disable map-side combine for the aggregation.
class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    null,
    false)
  with Serializable

class CoGroupedRDD[K](@transient rdds: Seq[RDD[(_, _)]], part: Partitioner)
  extends RDD[(K, Array[ArrayBuffer[Any]])](rdds.head.context) with Logging {

  val aggr = new CoGroupAggregator

  @transient
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        deps += new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        deps += new ShuffleDependency[Any, Any, ArrayBuffer[Any]](
            context.newShuffleId, rdd, Some(aggr), part)
      }
    }
    deps.toList
  }

  @transient
  val splits_ : Array[Split] = {
    val firstRdd = rdds.head
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId): CoGroupSplitDep
          case _ =>
            new NarrowCoGroupSplitDep(r, r.splits(i)): CoGroupSplitDep
        }
      }.toList)
    }
    array
  }

  override def splits = splits_

  override val partitioner = Some(part)

  override def preferredLocations(s: Split) = Nil

  override def compute(s: Split): Iterator[(K, Array[ArrayBuffer[Any]])] = {
    val split = s.asInstanceOf[CoGroupSplit]
    val numRdds = split.deps.size
    val map = new JHashMap[K, Array[ArrayBuffer[Any]]]
    def getSeq(k: K): Array[ArrayBuffer[Any]] = {
      var values = map.get(k)
      if (values == null) {
        values = Array.fill(numRdds)(new ArrayBuffer[Any])
        map.put(k, values)
      }
      values
    }
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit)) { getSeq(k.asInstanceOf[K])(depNum) += v }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        def mergePair(k: K, v: Any) { getSeq(k)(depNum) += v }
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[K, Any](shuffleId, split.index, mergePair)
      }
    }
    map.iterator
  }
}
