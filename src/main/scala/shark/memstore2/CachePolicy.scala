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
