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

import java.util.concurrent.ConcurrentHashMap
import java.util.LinkedHashMap
import java.util.Map.Entry

import scala.collection.JavaConversions._


/**
 * An general interface for pluggable cache eviction policies in Shark.
 * One example of usage is to control persistance levels of RDDs that represent a table's
 * Hive-partitions.
 */
trait CachePolicy[K, V] {

  protected var _loadFunc: (K => V) = _

  protected var _evictionFunc: (K, V) => Unit = _

  protected var _maxSize: Int = -1

  def initialize(
      strArgs: Array[String],
      fallbackMaxSize: Int,
      loadFunc: K => V,
      evictionFunc: (K, V) => Unit) {
    _loadFunc = loadFunc
    _evictionFunc = evictionFunc

    // By default, only initialize the `maxSize` from user specifications.
    strArgs.size match {
      case 0 => _maxSize = fallbackMaxSize
      case 1 => _maxSize = strArgs.head.toInt
      case _ =>
        throw new Exception("Accpted format: %s(maxSize: Int)".format(this.getClass.getName))
    }
    require(maxSize > 0, "Size given to cache eviction policy must be > 1")
  }

  def notifyPut(key: K, value: V): Unit

  def notifyRemove(key: K): Unit

  def notifyGet(key: K): Unit
  
  def keysOfCachedEntries: Seq[K]

  def maxSize: Int = _maxSize

  // TODO(harvey): Call this in Shark's handling of ALTER TABLE TBLPROPERTIES.
  def maxSize_= (newMaxSize: Int) = _maxSize = newMaxSize

  def hitRate: Double

  def evictionCount: Long
}


object CachePolicy {

  def instantiateWithUserSpecs[K, V](
      str: String,
      fallbackMaxSize: Int,
      loadFunc: K => V,
      evictionFunc: (K, V) => Unit): CachePolicy[K, V] = {
    val firstParenPos = str.indexOf('(')
    if (firstParenPos == -1) {
      val policy = Class.forName(str).newInstance.asInstanceOf[CachePolicy[K, V]]
      policy.initialize(Array.empty[String], fallbackMaxSize, loadFunc, evictionFunc)
      return policy
    } else {
      val classStr = str.slice(0, firstParenPos)
      val strArgs = str.substring(firstParenPos + 1, str.lastIndexOf(')')).split(',')
      val policy = Class.forName(classStr).newInstance.asInstanceOf[CachePolicy[K, V]]
      policy.initialize(strArgs, fallbackMaxSize, loadFunc, evictionFunc)
      return policy
    }
  }
}


/**
 * A cache that never evicts entries.
 */
class CacheAllPolicy[K, V] extends CachePolicy[K, V] {

  // Track the entries in the cache, so that keysOfCachedEntries() returns a valid result.
  var cache = new ConcurrentHashMap[K, V]()

  override def notifyPut(key: K, value: V) = cache.put(key, value)

  override def notifyRemove(key: K) = cache.remove(key)

  override def notifyGet(key: K) = Unit

  override def keysOfCachedEntries: Seq[K] = cache.keySet.toSeq

  override def hitRate = 1.0

  override def evictionCount = 0L
}


class LRUCachePolicy[K, V] extends LinkedMapBasedPolicy[K, V] {

  override def initialize(
      strArgs: Array[String],
      fallbackMaxSize: Int,
      loadFunc: K => V,
      evictionFunc: (K, V) => Unit) {
    super.initialize(strArgs, fallbackMaxSize, loadFunc, evictionFunc)
    _cache = new LinkedMapCache(true /* evictUsingAccessOrder */)
    _evictUsingAccessOrder = true
  }

}


class FIFOCachePolicy[K, V] extends LinkedMapBasedPolicy[K, V] {

  override def initialize(
      strArgs: Array[String],
      fallbackMaxSize: Int,
      loadFunc: K => V,
      evictionFunc: (K, V) => Unit) {
    super.initialize(strArgs, fallbackMaxSize, loadFunc, evictionFunc)
    _cache = new LinkedMapCache()
  }

}

sealed abstract class LinkedMapBasedPolicy[K, V] extends CachePolicy[K, V] {

  class LinkedMapCache(evictUsingAccessOrder: Boolean = false)
    extends LinkedHashMap[K, V](maxSize, 0.75F, evictUsingAccessOrder) {

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
  protected var _evictUsingAccessOrder = false

  override def initialize(
      strArgs: Array[String],
      fallbackMaxSize: Int,
      loadFunc: K => V,
      evictionFunc: (K, V) => Unit) {
    super.initialize(strArgs, fallbackMaxSize, loadFunc, evictionFunc)
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
      } else if (_evictUsingAccessOrder) {
        val loadedValue = _loadFunc(key)
        _cache.put(key, loadedValue)
        _missCount += 1L
      } else {
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
