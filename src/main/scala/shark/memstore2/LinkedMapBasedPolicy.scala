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

  override def initializeInternals(loadFunc: (K => V), evictionFunc: (K, V) => Unit) {
    super.initializeInternals(loadFunc, evictionFunc)
    _cache = new LinkedMapCache(true /* accessOrder */)
  }

}


class FIFOCachePolicy[K, V] extends LinkedMapBasedPolicy[K, V] {

  override def initializeInternals(loadFunc: (K => V), evictionFunc: (K, V) => Unit) {
    super.initializeInternals(loadFunc, evictionFunc)
    _cache = new LinkedMapCache()
  }

}


sealed abstract class LinkedMapBasedPolicy[K, V] extends CachePolicy[K, V] {

  class LinkedMapCache(accessOrder: Boolean = false)
    extends LinkedHashMap[K, V](maxSize, 0.75F, accessOrder) {

    override def removeEldestEntry(eldest: Entry[K, V]): Boolean = {
      val shouldRemove = (size() > maxSize)
      if (shouldRemove) {
        _evictionFunc(eldest.getKey, eldest.getValue)
        _evictionCount += 1
      }
      return shouldRemove
    }
  }

  protected var _cache: LinkedMapCache = _
  protected var _isInitialized = false
  protected var _hitCount: Long = 0L
  protected var _missCount: Long = 0L
  protected var _evictionCount: Long = 0L

  override def initializeInternals(loadFunc: (K => V), evictionFunc: (K, V) => Unit) {
    super.initializeInternals(loadFunc, evictionFunc)
    _isInitialized = true
  }

  override def notifyPut(key: K, value: V): Unit = {
    assert(_isInitialized, "Must initialize() %s.".format(this.getClass.getName))
    this.synchronized {
      val oldValue = _cache.put(key, value)
      if (oldValue != null) {
        _evictionFunc(key, oldValue)
        _evictionCount += 1
      }
    }
  }

  override def notifyRemove(key: K): Unit = {
    assert(_isInitialized, "Must initialize() %s.".format(this.getClass.getName))
    this.synchronized { _cache.remove(key) }
  }

  override def notifyGet(key: K): Unit = {
    assert(_isInitialized, "Must initialize() %s.".format(this.getClass.getName))
    this.synchronized {
      if (_cache.contains(key)) {
        _cache.get(key)
        _hitCount += 1L
      } else {
        val loadedValue = _loadFunc(key)
        _cache.put(key, loadedValue)
        _missCount += 1L
      }
    }
  }
  
  override def keysOfCachedEntries: Seq[K] = {
    assert(_isInitialized, "Must initialize() LRUCachePolicy.")
    this.synchronized {
      return _cache.keySet.toSeq
    }
  }

  override def hitRate: Double = {
    this.synchronized {
      val requestCount = _missCount + _hitCount
      val rate = if (requestCount == 0L) 1.0 else (_hitCount.toDouble / requestCount)
      return rate
    }
  }

  override def evictionCount = _evictionCount

}
