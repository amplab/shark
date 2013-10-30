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

package shark.memstore2

/*
 * Enumerations and static helper functions for caches supported by Shark.
 */
object CacheType extends Enumeration {

  /*
   * The CacheTypes:
   * - NONE: On-disk storage (e.g. a Hive table that is stored in HDFS ).
   * - HEAP: refers to Spark's block manager, which coordinates in-memory and on-disk RDD storage.
   * - TACHYON: A distributed storage system that manages an in-memory cache for sharing files and
   *            RDDs across cluster frameworks.
   */
  type CacheType = Value
  val NONE, HEAP, TACHYON = Value

  def shouldCache(c: CacheType): Boolean = (c != NONE)

  /** Get the cache type object from a string representation. */
  def fromString(name: String): CacheType = {
    if (name == null || name == "" || name.toLowerCase == "false") {
      NONE
    } else if (name.toLowerCase == "true") {
      HEAP
    } else {
      try {
        // Try to use Scala's Enumeration::withName() to interpret 'name'.
        withName(name.toUpperCase)
      } catch {
        case e: java.util.NoSuchElementException => throw new InvalidCacheTypeException(name)
      }
    }
  }

  class InvalidCacheTypeException(name: String)
    extends Exception("Invalid string representation of cache type " + name)
}
