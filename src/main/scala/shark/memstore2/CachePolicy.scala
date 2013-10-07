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

import com.google.common.cache._
import scala.collection.JavaConversions._


trait CachePolicy[K, V] {

  protected var maxSize: Long = _

  protected var loadFunc: (K => V) = _

  protected var evictionFunc: (K, V) => Unit = _

  def initialize(
	  maxSize: Long,
	  loadFunc: (K => V),
	  evictionFunc: (K, V) => Unit,
    shouldRecordStats: Boolean
    ): Unit = {
    this.maxSize = maxSize
    this.loadFunc = loadFunc
    this.evictionFunc = evictionFunc
  }
  
  def notifyPut(key: K, value: V): Unit

  def notifyRemove(key: K, value: V): Unit

  def notifyGet(key: K): Unit
  
  def getKeysOfCachedEntries: Seq[K]

  def getMaxSize = maxSize

  def getHitRate: Option[Double] = None

  def getEvictionCount: Option[Long] = None
}

class LRUCachePolicy[K <: AnyRef, V <: AnyRef] extends CachePolicy[K, V] {

  var isInitialized = false
  var hasRecordedStats = false
  var cache: LoadingCache[K, V] = _
  var cacheStats: Option[CacheStats] = None

  override def initialize(
	    maxSize: Long,
	    loadFunc: (K => V),
	    evictionFunc: (K, V) => Unit,
	    shouldRecordStats: Boolean
    ): Unit = {
    super.initialize(maxSize, loadFunc, evictionFunc, shouldRecordStats)

    var builder = CacheBuilder.newBuilder().maximumSize(maxSize)
    if (shouldRecordStats) {
      builder.recordStats()
      hasRecordedStats = true
    }

    val removalListener =
      new RemovalListener[K, V] {
        def onRemoval(removal: RemovalNotification[K, V]): Unit = {
          evictionFunc(removal.getKey, removal.getValue)
        }
      }
    val cacheLoader =
      new CacheLoader[K, V] {
        def load(key: K): V = loadFunc(key)
      }

    cache = builder
      .removalListener(removalListener)
      .build(cacheLoader)
    isInitialized = true
  }

  override def notifyPut(key: K, value: V): Unit = {
    assert(isInitialized, "Must initialize() LRUCachePolicy.")
    cache.put(key, value)
  }

  override def notifyRemove(key: K, value: V): Unit = {
    assert(isInitialized, "Must initialize() LRUCachePolicy.")
    cache.invalidate(key, value)
  }

  override def notifyGet(key: K): Unit = {
    assert(isInitialized, "Must initialize() LRUCachePolicy.")
    cache.get(key)
  }
  
  override def getKeysOfCachedEntries: Seq[K] = {
    assert(isInitialized, "Must initialize() LRUCachePolicy.")
    return cache.asMap.keySet.toSeq
  }

  override def getHitRate(): Option[Double] = {
    val hitRate = if (hasRecordedStats) Some(cache.stats.hitRate) else None
    return hitRate
  }

  override def getEvictionCount(): Option[Long] = {
    val evictionCount = if (hasRecordedStats) Some(cache.stats.evictionCount) else None
    return evictionCount
  }
}
