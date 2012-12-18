/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark

// RDD.elementClassManifest is spark package level visible.
// Should submit a patch to Spark to change that so we can create RDDs.


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
  override def compute(split: Split, context: TaskContext) = prev.iterator(split, context)
}
