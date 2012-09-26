package shark.memstore

import spark.ClosureCleaner
import spark.OneToOneDependency
import spark.RDD
import spark.Split


class SharkRDD[T: ClassManifest](self: RDD[T]) {

  def mapPartitionsWithSplit[U: ClassManifest](f: (Split, Iterator[T]) => Iterator[U]): RDD[U] =
    new MapPartitionsWithSplitRDD(self, clean(f))

  def pruneSplits(splitsFilterFunc: Split => Boolean): RDD[T] =
    new SplitsPruningRDD(self, splitsFilterFunc)

  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }
}


object SharkRDD {
  implicit def rddToSharkRdd[T](rdd: RDD[T]) = new SharkRDD(rdd)(rdd.elementClassManifest)
}


/**
 * A variant of the MapPartitionsRDD that passes the Split into the closure.
 * This can be used to generate or collect partition specific information such
 * as the number of tuples in a partition.
 */
class MapPartitionsWithSplitRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: (Split, Iterator[T]) => Iterator[U])
  extends RDD[U](prev.context) {

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = f(split, prev.iterator(split))
}


/**
 * A special RDD used to prune RDD splits so we can avoid launching tasks on
 * all splits. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on splits that don't have the range covering the key.
 */
class SplitsPruningRDD[T: ClassManifest](
    prev: RDD[T],
    splitsFilterFunc: Split => Boolean)
  extends RDD[T](prev.context) {

  @transient
  val _splits: Array[Split] = prev.splits.filter(splitsFilterFunc)

  override def splits = _splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split)
}
