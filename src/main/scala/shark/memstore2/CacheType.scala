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
   * - OFFHEAP: Stored in an off-heap data storage format, specified by the System property
   *            'shark.offheap.clientFactory'. Defaults to TachyonStorageClientFactory.
   * - NONE: Stored on disk (e.g., HDFS) and managed by Hive.
   */
  type CacheType = Value
  val MEMORY, MEMORY_ONLY, OFFHEAP, NONE = Value

  def shouldCache(c: CacheType): Boolean = (c != NONE)

  /** Get the cache type object from a string representation. */
  def fromString(name: String): CacheType = Option(name).map(_.toUpperCase) match {
    case None | Some("") | Some("FALSE") => NONE
    case Some("TRUE") => MEMORY
    case Some("HEAP") =>
      logWarning("The 'HEAP' cache type name is deprecated. Use 'MEMORY' instead.")
      MEMORY
    case Some("TACHYON") =>
      logWarning("The 'TACHYON' cache type name is deprecated. Use 'OFFHEAP' instead.")
      OFFHEAP
    case _ => {
      try {
        // Try to use Scala's Enumeration::withName() to interpret 'name'.
        withName(name.toUpperCase)
      } catch {
        case e: java.util.NoSuchElementException => throw new InvalidCacheTypeException(name)
      }
    }
  }

  class InvalidCacheTypeException(name: String)
    extends Exception("Invalid string representation of cache type: '%s'".format(name))
}
