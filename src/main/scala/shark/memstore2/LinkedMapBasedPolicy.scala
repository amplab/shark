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

import java.util.LinkedHashMap
import java.util.Map.Entry

import scala.collection.JavaConversions._

class LRUCachePolicy[K, V] extends LinkedMapBasedPolicy[K, V] {

  override def initialize(
      maxSize: Int,
      loadFunc: (K => V),
      evictionFunc: (K, V) => Unit
    ): Unit = {
    super.initialize(maxSize, loadFunc, evictionFunc)
    cache = new LinkedMapCache(true /* accessOrder */)
  }

}


class FIFOCachePolicy[K, V] extends LinkedMapBasedPolicy[K, V] {

  override def initialize(
      maxSize: Int,
      loadFunc: (K => V),
      evictionFunc: (K, V) => Unit
    ): Unit = {
    super.initialize(maxSize, loadFunc, evictionFunc)
    cache = new LinkedMapCache()
  }

}


sealed abstract class LinkedMapBasedPolicy[K, V] extends CachePolicy[K, V] {

  class LinkedMapCache(val accessOrder: Boolean = false)
    extends LinkedHashMap[K, V](maxSize, 0.75F, accessOrder) {

    override def removeEldestEntry(eldest: Entry[K, V]): Boolean = {
      evictionFunc(eldest.getKey, eldest.getValue)
      evictionCount += 1
      return (size() > maxSize)
    }
  }

  var cache: LinkedMapCache = _
  var isInitialized = false
  var hitCount: Long = 0L
  var missCount: Long = 0L
  var evictionCount: Long = 0L

  override def initialize(
      maxSize: Int,
      loadFunc: (K => V),
      evictionFunc: (K, V) => Unit
    ): Unit = {
    super.initialize(maxSize, loadFunc, evictionFunc)
    isInitialized = true
  }

  override def notifyPut(key: K, value: V): Unit = {
    assert(isInitialized, "Must initialize() %s.".format(this.getClass.getName))
    this.synchronized { cache.put(key, value) }
  }

  override def notifyRemove(key: K): Unit = {
    assert(isInitialized, "Must initialize() %s.".format(this.getClass.getName))
    this.synchronized { cache.remove(key) }
  }

  override def notifyGet(key: K): Unit = {
    assert(isInitialized, "Must initialize() %s.".format(this.getClass.getName))
    this.synchronized {
      if (cache.contains(key)) {
        cache.get(key)
        hitCount += 1L
      } else {
        val retrievedValue = loadFunc(key)
        cache.put(key, retrievedValue)
        missCount += 1L
      }
    }
  }
  
  override def getKeysOfCachedEntries: Seq[K] = {
    assert(isInitialized, "Must initialize() LRUCachePolicy.")
    return cache.keySet.toSeq
  }

  override def getHitRate: Double = {
    val requestCount = missCount + hitCount
    val hitRate = if (requestCount == 0L) 1.0 else (hitCount / requestCount)
    return hitRate
  }

  override def getEvictionCount = evictionCount

}
