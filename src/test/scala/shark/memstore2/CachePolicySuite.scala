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

import org.scalatest.FunSuite

import scala.collection.mutable.HashMap

class CachePolicySuite extends FunSuite {

  case class TestValue(var value: Int, var isCached: Boolean)

  class IdentifyKVGen(max: Int) {
    val kvMap = new HashMap[Int, TestValue]()
    for (i <- 0 until max) {
      kvMap(i) = TestValue(i, isCached = false)
    }

    def loadFunc(key: Int) = {
      val value = kvMap(key)
      value.isCached = true
      value
    }

    def evictionFunc(key: Int, value: TestValue) = {
      value.isCached = false
    }
  }

  test("LRU policy") {
    val kvGen = new IdentifyKVGen(20)
    val cacheSize = 10
    val lru = new LRUCachePolicy[Int, TestValue]()
    lru.initialize(Array.empty[String], cacheSize, kvGen.loadFunc _, kvGen.evictionFunc _)

    // Load KVs 0-9.
    (0 to 9).map(lru.notifyGet(_))
    assert(lru.keysOfCachedEntries.equals(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // Reorder access order by getting keys 2-4.
    (2 to 4).map(lru.notifyGet(_))
    assert(lru.keysOfCachedEntries.equals(Seq(0, 1, 5, 6, 7, 8, 9, 2, 3, 4)))

    // Get keys 10-12, which should evict (0, 1, 5).
    (10 to 12).map(lru.notifyGet(_))
    assert(lru.keysOfCachedEntries.equals(Seq(6, 7, 8, 9, 2, 3, 4, 10, 11, 12)))
    // Make sure the eviction function ran.
    assert(!kvGen.kvMap(0).isCached)
    assert(!kvGen.kvMap(1).isCached)
    assert(!kvGen.kvMap(5).isCached)

    // Reorder access order by getting keys (6, 8, 2).
    lru.notifyGet(6); lru.notifyGet(8); lru.notifyGet(2)
    assert(lru.keysOfCachedEntries.equals(Seq(7, 9, 3, 4, 10, 11, 12, 6, 8, 2)))

    // Remove 9, 4 and add 13, 14, 15. 7 should be evicted.
    lru.notifyRemove(9); lru.notifyRemove(4)
    (13 to 15).map(lru.notifyGet(_))
    assert(lru.keysOfCachedEntries.equals(Seq(3, 10, 11, 12, 6, 8, 2, 13, 14, 15)))
    assert(!kvGen.kvMap(7).isCached)
  }

  test("FIFO policy") {
    val kvGen = new IdentifyKVGen(15)
    val cacheSize = 5
    val fifo = new FIFOCachePolicy[Int, TestValue]()
    fifo.initialize(Array.empty[String], cacheSize, kvGen.loadFunc _, kvGen.evictionFunc _)

    // Load KVs 0-4.
    (0 to 4).map(fifo.notifyPut(_, TestValue(-1, true)))
    assert(fifo.keysOfCachedEntries.equals(Seq(0, 1, 2, 3, 4)))

    // Get 0-8, which should evict 0-3.
    (0 to 8).map(fifo.notifyPut(_, TestValue(-1, true)))
    assert(fifo.keysOfCachedEntries.equals(Seq(4, 5, 6, 7, 8)))

    // Remove 4, 6 and add 9-12. 5 and 7 should be evicted.
    fifo.notifyRemove(4); fifo.notifyRemove(6)
    (9 to 12).map(fifo.notifyPut(_, TestValue(-1, true)))
    assert(fifo.keysOfCachedEntries.equals(Seq(8, 9, 10, 11, 12)))
  }

  test("Policy classes instantiated from a string, with maxSize argument") {
    val kvGen = new IdentifyKVGen(15)
    val lruStr = "shark.memstore2.LRUCachePolicy(5)"
    val lru = CachePolicy.instantiateWithUserSpecs(
        lruStr, fallbackMaxSize = 10, kvGen.loadFunc _, kvGen.evictionFunc _)
    assert(lru.maxSize == 5)
    val fifoStr = "shark.memstore2.FIFOCachePolicy(5)"
    val fifo = CachePolicy.instantiateWithUserSpecs(
        fifoStr, fallbackMaxSize = 10, kvGen.loadFunc _, kvGen.evictionFunc _)
    assert(fifo.maxSize == 5)
  }

  test("Cache stats are recorded") {
    val kvGen = new IdentifyKVGen(20)
    val cacheSize = 5
    val lru = new LRUCachePolicy[Int, TestValue]()
    lru.initialize(Array.empty[String], cacheSize, kvGen.loadFunc _, kvGen.evictionFunc _)

    // Hit rate should start at 1.0
    assert(lru.hitRate == 1.0)

    (0 to 4).map(lru.notifyGet(_))
    assert(lru.hitRate == 0.0)

    // Get 1, 2, 3, which should bring the hit rate to 0.375.
    (1 to 3).map(lru.notifyGet(_))
    assert(lru.hitRate == 0.375)

    // Get 2-5, which brings the hit rate up to 0.50.
    (2 to 5).map(lru.notifyGet(_))
    assert(lru.evictionCount == 1)
    assert(lru.hitRate == 0.50)
  }
}