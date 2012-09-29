package shark.memstore

import spark.{OneToOneDependency, RDD, Split}

class EnhancedRDD[T: ClassManifest](self: RDD[T]) {
  def pruneSplits(splitsFilterFunc: Int => Boolean): RDD[T] =
    new SplitsPruningRDD(self, splitsFilterFunc)
}

object EnhancedRDD {
  implicit def rddToSharkRdd[T](rdd: RDD[T]) = new EnhancedRDD(rdd)(rdd.elementClassManifest)
}

/**
 * A special RDD used to prune RDD splits so we can avoid launching tasks on
 * all splits. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on splits that don't have the range covering the key.
 */
class SplitsPruningRDD[T: ClassManifest](
    prev: RDD[T],
    @transient splitsFilterFunc: Int => Boolean)
  extends RDD[T](prev.context) {

  @transient
  val _splits: Array[Split] = prev.splits.filter(s => splitsFilterFunc(s.index))

  override def splits = _splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split)
}
