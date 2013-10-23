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

import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD


trait CachePolicy[K, V] {

  protected var loadFunc: (K => V) = _

  protected var evictionFunc: (K, V) => Unit = _

  protected var maxSize: Int = -1

  def initialize(
	    maxSize: Int,
	    loadFunc: (K => V),
	    evictionFunc: (K, V) => Unit
    ): Unit = {
    this.maxSize = maxSize
    this.loadFunc = loadFunc
    this.evictionFunc = evictionFunc
  }

  def notifyPut(key: K, value: V): Unit

  def notifyRemove(key: K): Unit

  def notifyGet(key: K): Unit
  
  def getKeysOfCachedEntries: Seq[K]

  def getMaxSize = maxSize

  def getHitRate: Double

  def getEvictionCount: Long
}


class CacheAllPolicy[K, V] extends CachePolicy[K, V] {

  var keyToRdds = new ConcurrentHashMap[K, V]()

  override def notifyPut(key: K, value: V) = keyToRdds.put(key, value)

  override def notifyRemove(key: K) = keyToRdds.remove(key)

  override def notifyGet(key: K) = Unit

  override def getKeysOfCachedEntries: Seq[K] = keyToRdds.keySet.toSeq

  override def getHitRate: Double = 1.0

  override def getEvictionCount: Long = 0L
}
