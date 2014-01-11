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

import shark.LogHelper


/*
 * Enumerations and static helper functions for caches supported by Shark.
 */
object CacheType extends Enumeration with LogHelper {

  /*
   * The CacheTypes:
   * - MEMORY: Stored in memory and on disk (i.e., cache is write-through). Persistent across Shark
   *           sessions. By default, all such tables are reloaded into memory on restart.
   * - MEMORY_ONLY: Stored only in memory and dropped at the end of each Shark session.
   * - TACHYON: A distributed storage system that manages an in-memory cache for sharing files and
                RDDs across cluster frameworks.
   * - NONE: Stored on disk (e.g., HDFS) and managed by Hive.
   */
  type CacheType = Value
  val MEMORY, MEMORY_ONLY, TACHYON, NONE = Value

  def shouldCache(c: CacheType): Boolean = (c != NONE)

  /** Get the cache type object from a string representation. */
  def fromString(name: String): CacheType = {
    if (name == null || name == "" || name.toLowerCase == "false") {
      NONE
    } else if (name.toLowerCase == "true") {
      MEMORY
    } else {
      try {
        if (name.toUpperCase == "HEAP") {
          // Interpret 'HEAP' as 'MEMORY' to ensure backwards compatibility with Shark 0.8.0.
          logWarning("The 'HEAP' cache type name is deprecated. Use 'MEMORY' instead.")
          MEMORY
        } else {
          // Try to use Scala's Enumeration::withName() to interpret 'name'.
          withName(name.toUpperCase)
        }
      } catch {
        case e: java.util.NoSuchElementException => throw new InvalidCacheTypeException(name)
      }
    }
  }

  class InvalidCacheTypeException(name: String)
    extends Exception("Invalid string representation of cache type: '%s'".format(name))
}
