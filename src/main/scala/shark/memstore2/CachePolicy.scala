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


class CacheAllPolicy[K, V] extends CachePolicy[K, V] {

  var keyToRdds = new ConcurrentHashMap[K, V]()

  override def notifyPut(key: K, value: V) = keyToRdds.put(key, value)

  override def notifyRemove(key: K) = keyToRdds.remove(key)

  override def notifyGet(key: K) = Unit

  override def keysOfCachedEntries: Seq[K] = keyToRdds.keySet.toSeq

  override def hitRate = 1.0

  override def evictionCount = 0L
}


trait CachePolicy[K, V] {

  protected var _loadFunc: (K => V) = _

  protected var _evictionFunc: (K, V) => Unit = _

  protected var _maxSize: Int = -1

  def initializeWithUserSpecs(args: Array[String], fallbackMaxSize: Int) {
    // By default, only initialize the `maxSize` from user specifications.
    args.size match {
      case 0 => _maxSize = fallbackMaxSize
      case 1 => _maxSize = args.head.toInt
      case _ =>
        throw new Exception("Accpted format: %s(maxSize: Int)".format(this.getClass.getName))
    }
  }

  def initializeInternals(loadFunc: (K => V), evictionFunc: (K, V) => Unit) {
    require(maxSize > 0, "Must specify a maxSize before initializing CachePolicy internals.")
    _loadFunc = loadFunc
    _evictionFunc = evictionFunc
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

  def instantiateWithUserSpecs[K, V](str: String, fallbackMaxSize: Int): CachePolicy[K, V] = {
    val firstParenPos = str.indexOf('(')
    val classStr = str.slice(0, firstParenPos)
    val strArgs = str.substring(firstParenPos + 1, str.lastIndexOf(')')).split(',')
    val policy = Class.forName(classStr).newInstance.asInstanceOf[CachePolicy[K, V]]
    policy.initializeWithUserSpecs(strArgs, fallbackMaxSize)
    return policy
  }
}
