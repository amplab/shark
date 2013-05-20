package shark.execution

import spark.{RDD, SparkContext, SparkEnv, Partition, TaskContext}


/**
 * An RDD that is empty, i.e. has no element in it.
 *
 * TODO: Remove this once EmptyRDD is in Spark.
 */
class EmptyRDD[T: ClassManifest](sc: SparkContext) extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = Array.empty

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw new UnsupportedOperationException("empty RDD")
  }
}
