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


object CacheType extends Enumeration {

  type CacheType = Value
  val NONE, HEAP, TACHYON = Value

  def shouldCache(c: CacheType): Boolean = (c != NONE)

  /** Get the cache type object from a string representation. */
  def fromString(name: String): CacheType = {
    if (name == null || name == "") {
      NONE
    } else if (name.toLowerCase == "true") {
      HEAP
    } else {
      try {
        withName(name.toUpperCase)
      } catch {
        case e: java.util.NoSuchElementException => throw new InvalidCacheTypeException(name)
      }
    }
  }

  class InvalidCacheTypeException(name: String) extends Exception("Invalid cache type " + name)
}
