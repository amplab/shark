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

package shark

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.storage.StorageLevel

import shark.api.QueryExecutionException
import shark.memstore2.PartitionedMemoryTable


class CachePolicySuite extends FunSuite with BeforeAndAfterAll {

  val WAREHOUSE_PATH = TestUtils.getWarehousePath()
  val METASTORE_PATH = TestUtils.getMetastorePath()
  val MASTER = "local"

  var sc: SharkContext = _

  override def beforeAll() {
    sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)
    sc.runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" +
        METASTORE_PATH + ";create=true")
    sc.runSql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

    sc.runSql("set shark.test.data.path=" + TestUtils.dataFilePath)

    // test
    sc.runSql("drop table if exists test")
    sc.runSql("CREATE TABLE test (key INT, val STRING)")
    sc.runSql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test")
    sc.runSql("drop table if exists test_cached")
    sc.runSql("CREATE TABLE test_cached AS SELECT * FROM test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  private def createCachedPartitionedTable(
      tableName: String,
      maxSize: Int,
      numPartitionsToCreate: Int,
      cachePolicyClassName: String,
      shouldRecordStats: Boolean = false
    ): PartitionedMemoryTable = {
    sc.runSql("drop table if exists %s".format(tableName))
    sc.runSql("""
      create table %s(key int, value string)
        partitioned by (keypart int)
        tblproperties('shark.cache' = 'true',
                      'shark.cache.partition.cachePolicy.maxSize' = '%d',
                      'shark.cache.partition.cachePolicy.class' = '%s',
                      'shark.cache.storageLevel' = 'MEMORY_AND_DISK',
                      'shark.cache.partition.cachePolicy.shouldRecordStats' = '%b')
      """.format(
        tableName,
        maxSize,
        cachePolicyClassName,
        shouldRecordStats))
    var partitionNum = 0
    while (partitionNum < numPartitionsToCreate) {
      sc.runSql("""insert into table %s partition(keypart = %d)
        select * from test_cached""".format(tableName, partitionNum))
      partitionNum += 1
    }
    assert(SharkEnv.memoryMetadataManager.containsTable(tableName))
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(tableName).get
    return partitionedTable
  }

  test("shark.memstore2.LRUCachePolicy is the default policy") {
    val tableName = "lru_default_policy_cached"
    sc.runSql("""create table lru_default_policy_cached(key int, value string)
        partitioned by (keypart int)""")
    assert(SharkEnv.memoryMetadataManager.containsTable(tableName))
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(tableName).get
    val cachePolicy = partitionedTable.cachePolicy
    assert(cachePolicy.isInstanceOf[shark.memstore2.LRUCachePolicy[_, _]])
  }

  test("LRU: RDDs are evicted when the max size is reached.") {
    val tableName = "evict_partitions_maxSize"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* maxSize */,
      3 /* numPartitionsToCreate */,
      "shark.memstore2.LRUCachePolicy")
    val keypart1RDD = partitionedTable.getPartition("keypart=1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("""insert into table evict_partitions_maxSize partition(keypart = 4)
      select * from test""")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.NONE)
  }

  test("LRU: RDD eviction accounts for get()s.") {
    val tableName = "evict_partitions_with_get"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* maxSize */,
      3 /* numPartitionsToCreate */,
      "shark.memstore2.LRUCachePolicy")
    val keypart1RDD = partitionedTable.getPartition("keypart=1")
    val keypart2RDD = partitionedTable.getPartition("keypart=1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(keypart2RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("select count(1) from evict_partitions_with_get where keypart = 1")
    sc.runSql("""insert into table evict_partitions_with_get partition(keypart = 4)
      select * from test""")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(keypart2RDD.get.getStorageLevel == StorageLevel.NONE)
  }

  test("LRU: RDD eviction accounts for put()s.") {
    val tableName = "evict_partitions_with_put"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* maxSize */,
      3 /* numPartitionsToCreate */,
      "shark.memstore2.LRUCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(tableName))
    val keypart1RDD = partitionedTable.getPartition("keypart=1")
    val keypart2RDD = partitionedTable.getPartition("keypart=1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(keypart2RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("""insert into table evict_partitions_with_put partition(keypart = 1)
      select * from test""")
    sc.runSql("""insert into table evict_partitions_with_put partition(keypart = 4)
      select * from test""")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(keypart2RDD.get.getStorageLevel == StorageLevel.NONE)
  }

  test("LRU: get() reloads an RDD previously unpersist()'d.") {
    val tableName = "reload_evicted_partition"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* maxSize */,
      3 /* numPartitionsToCreate */,
      "shark.memstore2.LRUCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(tableName))
    val keypart1RDD = partitionedTable.getPartition("keypart=1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("""insert into table reload_evicted_partition partition(keypart = 4)
      select * from test""")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.NONE)

    // Scanning partition (keypart = 1) should reload the corresponding RDD into the cache, and
    // cause eviction of the RDD for partition (keypart = 2).
    sc.runSql("select count(1) from reload_evicted_partition where keypart = 1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    val keypart2RDD = partitionedTable.getPartition("keypart=1")
    assert(keypart2RDD.get.getStorageLevel == StorageLevel.NONE)
  }
  
  test("LRU: cache stats are not recorded by default") {
    val tableName = "dont_record_partition_cache_stats"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* maxSize */,
      1 /* numPartitionsToCreate */,
      "shark.memstore2.LRUCachePolicy",
      true /* shouldRecordStats */)
    val lruCachePolicy = partitionedTable.cachePolicy
    val hitRate = lruCachePolicy.getHitRate
    assert(hitRate.isEmpty)
    val evictionCount = lruCachePolicy.getEvictionCount
    assert(evictionCount.isEmpty)
  }

  test("LRU: record cache stats if user specifies it") {
    val tableName = "record_partition_cache_stats"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* maxSize */,
      1 /* numPartitionsToCreate */,
      "shark.memstore2.LRUCachePolicy",
      true /* shouldRecordStats */)
    val lruCachePolicy = partitionedTable.cachePolicy
    val hitRate = lruCachePolicy.getHitRate
    assert(hitRate.isDefined)
    assert(hitRate.get == 1.0)
    val evictionCount = lruCachePolicy.getEvictionCount
    assert(evictionCount.isDefined)
    assert(evictionCount.get == 0)
  }
}
