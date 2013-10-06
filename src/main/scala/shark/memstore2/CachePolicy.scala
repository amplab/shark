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


private[shark] abstract class CachePolicy[K, V] {

  var maxCacheSize: Long = _

  var loadFunction: (K => V) = _

  var evictionFunction: ((K, V) => Unit) = _

  def initialize(): Unit
  
  def notifyPut(key: K, value: V): Unit
  
  def notifyRemove(key: K, value: V): Unit

  def notifyGet(key: K): Unit
  
  def getKeysOfCachedEntries: Seq[K]
}

private[shark] class LRUCachePolicy[K <: AnyRef, V <: AnyRef] extends CachePolicy[K, V] {

  var cache: LoadingCache[K, V] = _

  override def initialize(): Unit = {
	var builder = CacheBuilder.newBuilder().maximumSize(maxCacheSize)

    val removalListener =
      new RemovalListener[K, V] {
        def onRemoval(removal: RemovalNotification[K, V]): Unit =
          evictionFunction(removal.getKey, removal.getValue)
      }
    val cacheLoader =
      new CacheLoader[K, V] {
        def load(key: K): V = loadFunction(key)
      }

    cache = builder.removalListener(removalListener).build(cacheLoader)
  }

  override def notifyPut(key: K, value: V): Unit = {
    cache.put(key, value)
  }

  override def notifyRemove(key: K, value: V): Unit = {
    cache.invalidate(key, value)
  }

  override def notifyGet(key: K): Unit = {
    cache.get(key)
  }
  
  protected def getKeysOfCachedEntries: Seq[K] = cache.asMap.keySet.toSeq

}
