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

package shark.api

import scala.reflect.ClassTag

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


class JavaTableRDD(val rdd: RDD[Row], val schema: Array[ColumnDesc])
  extends JavaRDDLike[Row, JavaTableRDD] {

  override def wrapRDD(rdd: RDD[Row]): JavaTableRDD = new JavaTableRDD(rdd, schema)

  // Common RDD functions
  override val classTag: ClassTag[Row] = implicitly[ClassTag[Row]]

  // This shouldn't be necessary, but we seem to need this to get first() to return Row
  // instead of Object; possibly a compiler bug?
  override def first(): Row = rdd.first()

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaTableRDD = wrapRDD(rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): JavaTableRDD = wrapRDD(rdd.persist(newLevel))

  // Transformations (return a new RDD)

  // Note: we didn't implement distinct() because equals() and hashCode() are not defined for Row.

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[Row, java.lang.Boolean]): JavaTableRDD =
    wrapRDD(rdd.filter((x => f(x).booleanValue())))

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaTableRDD =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   *
   * Note: the `schema` of a union is this RDD's schema.
   */
  def union(other: JavaTableRDD): JavaTableRDD = wrapRDD(rdd.union(other.rdd))

}


