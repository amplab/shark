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

import scala.collection.JavaConversions._

import org.scalatest.FunSuite

import org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel

import shark.api.QueryExecutionException
import shark.memstore2.{CacheType, MemoryMetadataManager, PartitionedMemoryTable}
import shark.tgf.{RDDSchema, Schema}
// import expectSql() shortcut methods
import shark.SharkRunner._


class SQLSuite extends FunSuite {

  val DEFAULT_DB_NAME = DEFAULT_DATABASE_NAME
  val KV1_TXT_PATH = "${hiveconf:shark.test.data.path}/kv1.txt"

  var sc: SharkContext = SharkRunner.init()
  var sharkMetastore: MemoryMetadataManager = SharkEnv.memoryMetadataManager

  private def createCachedPartitionedTable(
      tableName: String,
      numPartitionsToCreate: Int,
      maxCacheSize: Int = 10,
      cachePolicyClassName: String = "shark.memstore2.LRUCachePolicy"
    ): PartitionedMemoryTable = {
    sc.runSql("drop table if exists %s".format(tableName))
    sc.runSql("""
      create table %s(key int, value string)
        partitioned by (keypart int)
        tblproperties('shark.cache' = 'true',
                      'shark.cache.policy.maxSize' = '%d',
                      'shark.cache.policy' = '%s')
      """.format(
        tableName,
        maxCacheSize,
        cachePolicyClassName))
    var partitionNum = 1
    while (partitionNum <= numPartitionsToCreate) {
      sc.runSql("""insert into table %s partition(keypart = %d)
        select * from test_cached""".format(tableName, partitionNum))
      partitionNum += 1
    }
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
      DEFAULT_DB_NAME, tableName).get
    partitionedTable
  }

  def isFlattenedUnionRDD(unionRDD: UnionRDD[_]) = {
    unionRDD.rdds.find(_.isInstanceOf[UnionRDD[_]]).isEmpty
  }

  // Takes a sum over the table's 'key' column, for both the cached contents and the copy on disk.
  def expectUnifiedKVTable(
      cachedTableName: String,
      partSpecOpt: Option[Map[String, String]] = None) {
    // Check that the table is in memory and is a unified view.
    val sharkTableOpt = sharkMetastore.getTable(DEFAULT_DB_NAME, cachedTableName)
    assert(sharkTableOpt.isDefined, "Table %s cannot be found in the Shark metastore")
    assert(sharkTableOpt.get.cacheMode == CacheType.MEMORY,
      "'shark.cache' field for table %s is not CacheType.MEMORY")

    // Load a non-cached copy of the table into memory.
    val cacheSum = sc.sql("select sum(key) from %s".format(cachedTableName))(0)
    val hiveTable = Hive.get().getTable(DEFAULT_DB_NAME, cachedTableName)
    val location = partSpecOpt match {
      case Some(partSpec) => {
        val partition = Hive.get().getPartition(hiveTable, partSpec, false /* forceCreate */)
        partition.getDataLocation.toString
      }
      case None => hiveTable.getDataLocation.toString
    }
    // Create a table with contents loaded from the table's data directory.
    val diskTableName = "%s_disk_copy".format(cachedTableName)
    sc.sql("drop table if exists %s".format(diskTableName))
    sc.sql("create table %s (key int, value string)".format(diskTableName))
    sc.sql("load data local inpath '%s' into table %s".format(location, diskTableName))
    val diskSum = sc.sql("select sum(key) from %s".format(diskTableName))(0)
    assert(diskSum == cacheSum, "Sum of keys from cached and disk contents differ")
  }

  //////////////////////////////////////////////////////////////////////////////
  // basic SQL
  //////////////////////////////////////////////////////////////////////////////
  test("count") {
    expectSql("select count(*) from test", "500")
    expectSql("select count(*) from test_cached", "500")
  }

  test("filter") {
    expectSql("select * from test where key=100 or key=497",
      Array("100\tval_100", "100\tval_100", "497\tval_497"))
    expectSql("select * from test_cached where key=100 or key=497",
      Array("100\tval_100", "100\tval_100", "497\tval_497"))
  }

  test("count distinct") {
    sc.runSql("set mapred.reduce.tasks=3")
    expectSql("select count(distinct key) from test", "309")
    expectSql("select count(distinct key) from test_cached", "309")
    expectSql(
      """|SELECT substr(key,1,1), count(DISTINCT substr(val,5)) from test
         |GROUP BY substr(key,1,1)""".stripMargin,
      Array("0\t1", "1\t71", "2\t69", "3\t62", "4\t74", "5\t6", "6\t5", "7\t6", "8\t8", "9\t7"))
  }

  test("count bigint") {
    sc.runSql("drop table if exists test_bigint")
    sc.runSql("create table test_bigint (key bigint, val string)")
    sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/kv1.txt'
      OVERWRITE INTO TABLE test_bigint""")
    sc.runSql("drop table if exists test_bigint_cached")
    sc.runSql("create table test_bigint_cached as select * from test_bigint")
    expectSql("select val, count(*) from test_bigint_cached where key=484 group by val",
      "val_484\t1")
  }

  test("limit") {
    assert(sc.runSql("select * from test limit 10").results.length === 10)
    assert(sc.runSql("select * from test limit 501").results.length === 500)
    sc.runSql("drop table if exists test_limit0")
    assert(sc.runSql("select * from test limit 0").results.length === 0)
    assert(sc.runSql("create table test_limit0 as select * from test limit 0").results.length === 0)
    assert(sc.runSql("select * from test_limit0 limit 0").results.length === 0)
    assert(sc.runSql("select * from test_limit0 limit 1").results.length === 0)
  }

  //////////////////////////////////////////////////////////////////////////////
  // sorting
  //////////////////////////////////////////////////////////////////////////////

  ignore("full order by") {
    expectSql("select * from users order by id", Array("1\tA", "2\tB", "3\tA"), sort = false)
    expectSql("select * from users order by id desc", Array("3\tA", "2\tB", "1\tA"), sort = false)
    expectSql("select * from users order by name, id", Array("1\tA", "3\tA", "2\tB"), sort = false)
    expectSql("select * from users order by name desc, id desc", Array("2\tB", "3\tA", "1\tA"),
      sort = false)
  }

  test("full order by with limit") {
    expectSql("select * from users order by id limit 2", Array("1\tA", "2\tB"), sort = false)
    expectSql("select * from users order by id desc limit 2", Array("3\tA", "2\tB"), sort = false)
    expectSql("select * from users order by name, id limit 2", Array("1\tA", "3\tA"), sort = false)
    expectSql("select * from users order by name desc, id desc limit 2", Array("2\tB", "3\tA"),
      sort = false)
  }

  //////////////////////////////////////////////////////////////////////////////
  // join
  //////////////////////////////////////////////////////////////////////////////
  test("join ouput rows of stand objects") {
    assert(
      sc.sql("select test1val from users join test1 on users.id=test1.id and users.id=1").head ===
      "[0,1,2]")
  }

  //////////////////////////////////////////////////////////////////////////////
  // map join
  //////////////////////////////////////////////////////////////////////////////
  test("map join") {
    expectSql("""select u.name, count(c.click) from clicks c join users u on (c.id = u.id)
      group by u.name having u.name='A'""",
      "A\t3")
  }

  test("map join2") {
    expectSql("select count(*) from clicks join users on (clicks.id = users.id)", "5")
  }

  //////////////////////////////////////////////////////////////////////////////
  // join
  //////////////////////////////////////////////////////////////////////////////
  test("outer join on null key") {
    expectSql("""select count(distinct a.val) from
        (select * from test_null where key is null) a
        left outer join
        (select * from test_null where key is null) b on a.key=b.key""", "7")
  }

  //////////////////////////////////////////////////////////////////////////////
  // cache DDL
  //////////////////////////////////////////////////////////////////////////////
  test("Use regular CREATE TABLE and '_cached' suffix to create cached table") {
    sc.runSql("drop table if exists empty_table_cached")
    sc.runSql("create table empty_table_cached(key string, value string)")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "empty_table_cached"))
    assert(!SharkEnv.memoryMetadataManager.isHivePartitioned(DEFAULT_DB_NAME, "empty_table_cached"))
  }

  test("Use regular CREATE TABLE and table properties to create cached table") {
    sc.runSql("drop table if exists empty_table_cached_tbl_props")
    sc.runSql("""create table empty_table_cached_tbl_props(key string, value string)
      TBLPROPERTIES('shark.cache' = 'true')""")
    assert(SharkEnv.memoryMetadataManager.containsTable(
      DEFAULT_DB_NAME, "empty_table_cached_tbl_props"))
    assert(!SharkEnv.memoryMetadataManager.isHivePartitioned(
      DEFAULT_DB_NAME, "empty_table_cached_tbl_props"))
  }

  test("Insert into empty cached table") {
    sc.runSql("drop table if exists new_table_cached")
    sc.runSql("create table new_table_cached(key string, value string)")
    sc.runSql("insert into table new_table_cached select * from test where key > -1 limit 499")
    expectSql("select count(*) from new_table_cached", "499")
  }

  test("rename cached table") {
    sc.runSql("drop table if exists test_oldname_cached")
    sc.runSql("drop table if exists test_rename")
    sc.runSql("create table test_oldname_cached as select * from test")
    sc.runSql("alter table test_oldname_cached rename to test_rename")

    assert(!SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "test_oldname_cached"))
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "test_rename"))

    expectSql("select count(*) from test_rename", "500")
  }

  test("insert into cached tables") {
    sc.runSql("drop table if exists test1_cached")
    sc.runSql("create table test1_cached as select * from test")
    expectSql("select count(*) from test1_cached", "500")
    sc.runSql("insert into table test1_cached select * from test where key > -1 limit 499")
    expectSql("select count(*) from test1_cached", "999")
  }

  test("insert overwrite") {
    sc.runSql("drop table if exists test2_cached")
    sc.runSql("create table test2_cached as select * from test")
    expectSql("select count(*) from test2_cached", "500")
    sc.runSql("insert overwrite table test2_cached select * from test where key > -1 limit 499")
    expectSql("select count(*) from test2_cached", "499")
  }

  test("error when attempting to update cached table(s) using command with multiple INSERTs") {
    sc.runSql("drop table if exists multi_insert_test")
    sc.runSql("drop table if exists multi_insert_test_cached")
    sc.runSql("create table multi_insert_test as select * from test")
    sc.runSql("create table multi_insert_test_cached as select * from test")
    intercept[QueryExecutionException] {
      sc.runSql("""from test
        insert into table multi_insert_test select *
        insert into table multi_insert_test_cached select *""")
    }
  }

  test("create cached table with 'shark.cache' flag in table properties") {
    sc.runSql("drop table if exists ctas_tbl_props")
    sc.runSql("""create table ctas_tbl_props TBLPROPERTIES ('shark.cache'='true') as
      select * from test""")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "ctas_tbl_props"))
    expectSql("select * from ctas_tbl_props where key=407", "407\tval_407")
  }

  test("default to Hive table creation when 'shark.cache' flag is false in table properties") {
    sc.runSql("drop table if exists ctas_tbl_props_should_not_be_cached")
    sc.runSql("""
      CREATE TABLE ctas_tbl_props_result_should_not_be_cached
        TBLPROPERTIES ('shark.cache'='false')
        AS select * from test""")
    assert(!SharkEnv.memoryMetadataManager.containsTable(
      DEFAULT_DB_NAME, "ctas_tbl_props_should_not_be_cached"))
  }

  test("cached tables with complex types") {
    sc.runSql("drop table if exists test_complex_types")
    sc.runSql("drop table if exists test_complex_types_cached")
    sc.runSql("""CREATE TABLE test_complex_types (
      a STRING, b ARRAY<STRING>, c ARRAY<MAP<STRING,STRING>>, d MAP<STRING,ARRAY<STRING>>)""")
    sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/create_nested_type.txt'
      overwrite into table test_complex_types""")
    sc.runSql("""create table test_complex_types_cached TBLPROPERTIES ("shark.cache" = "true") as
      select * from test_complex_types""")

    assert(sc.sql("select a from test_complex_types_cached where a = 'a0'").head === "a0")

    assert(sc.sql("select b from test_complex_types_cached where a = 'a0'").head ===
      """["b00","b01"]""")

    assert(sc.sql("select c from test_complex_types_cached where a = 'a0'").head ===
      """[{"c001":"C001","c002":"C002"},{"c011":null,"c012":"C012"}]""")

    assert(sc.sql("select d from test_complex_types_cached where a = 'a0'").head ===
      """{"d01":["d011","d012"],"d02":["d021","d022"]}""")

    assert(SharkEnv.memoryMetadataManager.containsTable(
      DEFAULT_DB_NAME, "test_complex_types_cached"))
  }

  test("disable caching by default") {
    sc.runSql("set shark.cache.flag.checkTableName=false")
    sc.runSql("drop table if exists should_not_be_cached")
    sc.runSql("create table should_not_be_cached as select * from test")
    expectSql("select key from should_not_be_cached where key = 407", "407")
    assert(!SharkEnv.memoryMetadataManager.containsTable(
      DEFAULT_DB_NAME, "should_not_be_cached"))
    sc.runSql("set shark.cache.flag.checkTableName=true")
  }

  test("cached table name should be case-insensitive") {
    sc.runSql("drop table if exists sharkTest5Cached")
    sc.runSql("""create table sharkTest5Cached TBLPROPERTIES ("shark.cache" = "true") as
      select * from test""")
    expectSql("select val from sharktest5Cached where key = 407", "val_407")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "sharkTest5Cached"))
  }

  test("dropping cached tables should clean up RDDs") {
    sc.runSql("drop table if exists sharkTest5Cached")
    sc.runSql("""create table sharkTest5Cached TBLPROPERTIES ("shark.cache" = "true") as
      select * from test""")
    sc.runSql("drop table sharkTest5Cached")
    assert(!SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "sharkTest5Cached"))
  }

  test("lateral view explode column pruning") {
    // If column pruner doesn't take lateral view into account, the first result will be null.
    assert(sc.runSql("""select * from test_cached
      lateral view explode(array(1, 2, 3)) exploadedTbl as col1""").results.head.head != null)
  }

  //////////////////////////////////////////////////////////////////////////////
  // Caching Hive-partititioned tables
  // Note: references to 'partition' for this section refer to a Hive-partition.
  //////////////////////////////////////////////////////////////////////////////
  test("Use regular CREATE TABLE and '_cached' suffix to create cached, partitioned table") {
    sc.runSql("drop table if exists empty_part_table_cached")
    sc.runSql("""create table empty_part_table_cached(key int, value string)
      partitioned by (keypart int)""")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "empty_part_table_cached"))
    assert(SharkEnv.memoryMetadataManager.isHivePartitioned(
      DEFAULT_DB_NAME, "empty_part_table_cached"))
  }

  test("Use regular CREATE TABLE and table properties to create cached, partitioned table") {
    sc.runSql("drop table if exists empty_part_table_cached_tbl_props")
    sc.runSql("""create table empty_part_table_cached_tbl_props(key int, value string)
      partitioned by (keypart int) tblproperties('shark.cache' = 'true')""")
    assert(SharkEnv.memoryMetadataManager.containsTable(
      DEFAULT_DB_NAME, "empty_part_table_cached_tbl_props"))
    assert(SharkEnv.memoryMetadataManager.isHivePartitioned(
      DEFAULT_DB_NAME, "empty_part_table_cached_tbl_props"))
  }

  test("alter cached table by adding a new partition") {
    sc.runSql("drop table if exists alter_part_cached")
    sc.runSql("""create table alter_part_cached(key int, value string)
      partitioned by (keypart int)""")
    sc.runSql("""alter table alter_part_cached add partition(keypart = 1)""")
    val tableName = "alter_part_cached"
    val partitionColumn = "keypart=1"
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
      DEFAULT_DB_NAME, tableName).get
    assert(partitionedTable.containsPartition(partitionColumn))
  }

  test("alter cached table by dropping a partition") {
    sc.runSql("drop table if exists alter_drop_part_cached")
    sc.runSql("""create table alter_drop_part_cached(key int, value string)
      partitioned by (keypart int)""")
    sc.runSql("""alter table alter_drop_part_cached add partition(keypart = 1)""")
    val tableName = "alter_drop_part_cached"
    val partitionColumn = "keypart=1"
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
      DEFAULT_DB_NAME, tableName).get
    assert(partitionedTable.containsPartition(partitionColumn))
    sc.runSql("""alter table alter_drop_part_cached drop partition(keypart = 1)""")
    assert(!partitionedTable.containsPartition(partitionColumn))
  }

  test("insert into a partition of a cached table") {
    val tableName = "insert_part_cached"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      1 /* numPartitionsToCreate */)
    expectSql("select value from insert_part_cached where key = 407 and keypart = 1", "val_407")

  }

  test("insert overwrite a partition of a cached table") {
    val tableName = "insert_over_part_cached"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      1 /* numPartitionsToCreate */)
    expectSql("""select value from insert_over_part_cached
      where key = 407 and keypart = 1""", "val_407")
    sc.runSql("""insert overwrite table insert_over_part_cached partition(keypart = 1)
      select key, -1 from test""")
    expectSql("select value from insert_over_part_cached where key = 407 and keypart = 1", "-1")
  }

  test("scan cached, partitioned table that's empty") {
    sc.runSql("drop table if exists empty_part_table_cached")
    sc.runSql("""create table empty_part_table_cached(key int, value string)
      partitioned by (keypart int)""")
    expectSql("select count(*) from empty_part_table_cached", "0")
  }

  test("scan cached, partitioned table that has a single partition") {
    val tableName = "scan_single_part_cached"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      1 /* numPartitionsToCreate */)
    expectSql("select * from scan_single_part_cached where key = 407", "407\tval_407\t1")
  }

  test("scan cached, partitioned table that has multiple partitions") {
    val tableName = "scan_mult_part_cached"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */)
    expectSql("select * from scan_mult_part_cached where key = 407 order by keypart",
      Array("407\tval_407\t1", "407\tval_407\t2", "407\tval_407\t3"))
  }

  test("drop/unpersist cached, partitioned table that has multiple partitions") {
    val tableName = "drop_mult_part_cached"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */)
    val keypart1RDD = partitionedTable.getPartition("keypart=1")
    val keypart2RDD = partitionedTable.getPartition("keypart=2")
    val keypart3RDD = partitionedTable.getPartition("keypart=3")
    sc.runSql("drop table drop_mult_part_cached ")
    assert(!SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    // All RDDs should have been unpersisted.
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.NONE)
    assert(keypart2RDD.get.getStorageLevel == StorageLevel.NONE)
    assert(keypart3RDD.get.getStorageLevel == StorageLevel.NONE)
  }

  test("drop cached partition represented by a UnionRDD (i.e., the result of multiple inserts)") {
    val tableName = "drop_union_part_cached"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      1 /* numPartitionsToCreate */)
    sc.runSql("insert into table drop_union_part_cached partition(keypart = 1) select * from test")
    sc.runSql("insert into table drop_union_part_cached partition(keypart = 1) select * from test")
    sc.runSql("insert into table drop_union_part_cached partition(keypart = 1) select * from test")
    val keypart1RDD = partitionedTable.getPartition("keypart=1")
    sc.runSql("drop table drop_union_part_cached")
    assert(!SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    // All RDDs should have been unpersisted.
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.NONE)
  }

  //////////////////////////////////////////////////////////////////////////////
  // RDD(partition) eviction policy for cached Hive-partititioned tables
  //////////////////////////////////////////////////////////////////////////////

  test("shark.memstore2.CacheAllPolicy is the default policy") {
    val tableName = "default_policy_cached"
    sc.runSql("""create table default_policy_cached(key int, value string)
        partitioned by (keypart int)""")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
      DEFAULT_DB_NAME, tableName).get
    val cachePolicy = partitionedTable.cachePolicy
    assert(cachePolicy.isInstanceOf[shark.memstore2.CacheAllPolicy[_, _]])
  }

  test("LRU: RDDs are not evicted if the cache isn't full.") {
    val tableName = "evict_partitions_maxSize"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      2 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    val keypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)
  }

  test("LRU: RDDs are evicted when the max size is reached.") {
    val tableName = "evict_partitions_maxSize"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    val keypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("""insert into table evict_partitions_maxSize partition(keypart = 4)
      select * from test""")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.NONE)
  }

  test("LRU: RDD eviction accounts for partition scans - a cache.get()") {
    val tableName = "evict_partitions_with_get"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    val keypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    val keypart2RDD = partitionedTable.keyToPartitions.get("keypart=2")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)
    assert(TestUtils.getStorageLevelOfRDD(keypart2RDD.get) == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("select count(1) from evict_partitions_with_get where keypart = 1")
    sc.runSql("""insert into table evict_partitions_with_get partition(keypart = 4)
      select * from test""")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)

    assert(TestUtils.getStorageLevelOfRDD(keypart2RDD.get) == StorageLevel.NONE)
  }

  test("LRU: RDD eviction accounts for INSERT INTO - a cache.get().") {
    val tableName = "evict_partitions_insert_into"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val oldKeypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    val keypart2RDD = partitionedTable.keyToPartitions.get("keypart=2")
    assert(TestUtils.getStorageLevelOfRDD(oldKeypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)
    assert(TestUtils.getStorageLevelOfRDD(keypart2RDD.get) == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("""insert into table evict_partitions_insert_into partition(keypart = 1)
      select * from test""")
    sc.runSql("""insert into table evict_partitions_insert_into partition(keypart = 4)
      select * from test""")
    assert(TestUtils.getStorageLevelOfRDD(oldKeypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)
    val newKeypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    assert(TestUtils.getStorageLevelOfRDD(newKeypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)

    val keypart2StorageLevel = TestUtils.getStorageLevelOfRDD(keypart2RDD.get)
    assert(keypart2StorageLevel == StorageLevel.NONE)
  }

  test("LRU: RDD eviction accounts for INSERT OVERWRITE - a cache.put()") {
    val tableName = "evict_partitions_insert_overwrite"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val oldKeypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    val keypart2RDD = partitionedTable.keyToPartitions.get("keypart=2")
    assert(TestUtils.getStorageLevelOfRDD(oldKeypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)
    assert(TestUtils.getStorageLevelOfRDD(keypart2RDD.get) == StorageLevel.MEMORY_AND_DISK)
    sc.runSql("""insert overwrite table evict_partitions_insert_overwrite partition(keypart = 1)
      select * from test""")
    sc.runSql("""insert into table evict_partitions_insert_overwrite partition(keypart = 4)
      select * from test""")
    assert(TestUtils.getStorageLevelOfRDD(oldKeypart1RDD.get) == StorageLevel.NONE)
    val newKeypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    assert(TestUtils.getStorageLevelOfRDD(newKeypart1RDD.get) == StorageLevel.MEMORY_AND_DISK)

    val keypart2StorageLevel = TestUtils.getStorageLevelOfRDD(keypart2RDD.get)
    assert(keypart2StorageLevel == StorageLevel.NONE)
  }

  test("LRU: RDD eviction accounts for ALTER TABLE DROP PARTITION - a cache.remove()") {
    val tableName = "evict_partitions_removals"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    sc.runSql("alter table evict_partitions_removals drop partition(keypart = 1)")
    sc.runSql("""insert into table evict_partitions_removals partition(keypart = 4)
      select * from test""")
    sc.runSql("""insert into table evict_partitions_removals partition(keypart = 5)
      select * from test""")
    val keypart2RDD = partitionedTable.keyToPartitions.get("keypart=2")
    assert(TestUtils.getStorageLevelOfRDD(keypart2RDD.get) == StorageLevel.NONE)
  }

  test("LRU: get() reloads an RDD previously unpersist()'d.") {
    val tableName = "reload_evicted_partition"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.LRUCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val keypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    val lvl = TestUtils.getStorageLevelOfRDD(keypart1RDD.get)
    assert(lvl == StorageLevel.MEMORY_AND_DISK, "got: " + lvl)
    sc.runSql("""insert into table reload_evicted_partition partition(keypart = 4)
      select * from test""")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.NONE)

    // Scanning partition (keypart = 1) should reload the corresponding RDD into the cache, and
    // cause eviction of the RDD for partition (keypart = 2).
    sc.runSql("select count(1) from reload_evicted_partition where keypart = 1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.MEMORY_AND_DISK)
    val keypart2RDD = partitionedTable.keyToPartitions.get("keypart=2")
    val keypart2StorageLevel = TestUtils.getStorageLevelOfRDD(keypart2RDD.get)
    assert(keypart2StorageLevel == StorageLevel.NONE,
      "StorageLevel for partition(keypart=2) should be NONE, but got: " + keypart2StorageLevel)
  }

  test("FIFO: get() does not reload an RDD previously unpersist()'d.") {
    val tableName = "dont_reload_evicted_partition"
    val partitionedTable = createCachedPartitionedTable(
      tableName,
      3 /* numPartitionsToCreate */,
      3 /* maxCacheSize */,
      "shark.memstore2.FIFOCachePolicy")
    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, tableName))
    val keypart1RDD = partitionedTable.keyToPartitions.get("keypart=1")
    val lvl = TestUtils.getStorageLevelOfRDD(keypart1RDD.get)
    assert(lvl == StorageLevel.MEMORY_AND_DISK, "got: " + lvl)
    sc.runSql("""insert into table dont_reload_evicted_partition partition(keypart = 4)
      select * from test""")
    assert(TestUtils.getStorageLevelOfRDD(keypart1RDD.get) == StorageLevel.NONE)

    // Scanning partition (keypart = 1) should reload the corresponding RDD into the cache, and
    // cause eviction of the RDD for partition (keypart = 2).
    sc.runSql("select count(1) from dont_reload_evicted_partition where keypart = 1")
    assert(keypart1RDD.get.getStorageLevel == StorageLevel.NONE, "got: " +  keypart1RDD.get.getStorageLevel)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Prevent nested UnionRDDs - those should be "flattened" in MemoryStoreSinkOperator.
  ///////////////////////////////////////////////////////////////////////////////////////

  test("flatten UnionRDDs") {
    sc.sql("create table flat_cached as select * from test_cached")
    sc.sql("insert into table flat_cached select * from test")
    val tableName = "flat_cached"
    var memoryTable = SharkEnv.memoryMetadataManager.getMemoryTable(DEFAULT_DB_NAME, tableName).get
    var unionRDD = memoryTable.getRDD.get.asInstanceOf[UnionRDD[_]]
    val numParentRDDs = unionRDD.rdds.size
    assert(isFlattenedUnionRDD(unionRDD))

    // Insert another set of query results. The flattening should kick in here.
    sc.sql("insert into table flat_cached select * from test")
    unionRDD = memoryTable.getRDD.get.asInstanceOf[UnionRDD[_]]
    assert(isFlattenedUnionRDD(unionRDD))
    assert(unionRDD.rdds.size == numParentRDDs + 1)
  }

  test("flatten UnionRDDs for partitioned tables") {
    sc.sql("drop table if exists part_table_cached")
    sc.sql("""create table part_table_cached(key int, value string)
      partitioned by (keypart int)""")
    sc.sql("alter table part_table_cached add partition(keypart = 1)")
    sc.sql("insert into table part_table_cached partition(keypart = 1) select * from flat_cached")
    val tableName = "part_table_cached"
    val partitionKey = "keypart=1"
    val partitionedTable = SharkEnv.memoryMetadataManager.getPartitionedTable(
      DEFAULT_DB_NAME, tableName).get
    var unionRDD = partitionedTable.keyToPartitions.get(partitionKey).get.asInstanceOf[UnionRDD[_]]
    val numParentRDDs = unionRDD.rdds.size
    assert(isFlattenedUnionRDD(unionRDD))

    // Insert another set of query results into the same partition.
    // The flattening should kick in here.
    sc.runSql("insert into table part_table_cached partition(keypart = 1) select * from flat_cached")
    unionRDD = partitionedTable.getPartition(partitionKey).get.asInstanceOf[UnionRDD[_]]
    assert(isFlattenedUnionRDD(unionRDD))
    assert(unionRDD.rdds.size == numParentRDDs + 1)
  }

  //////////////////////////////////////////////////////////////////////////////
  // Tableau bug
  //////////////////////////////////////////////////////////////////////////////

  test("tableau bug / adw") {
    sc.sql("drop table if exists adw")
    sc.sql("""create table adw TBLPROPERTIES ("shark.cache" = "true") as
      select cast(key as int) as k, val from test""")
    expectSql("select count(k) from adw where val='val_487' group by 1 having count(1) > 0", "1")
  }

   //////////////////////////////////////////////////////////////////////////////
  // Partition pruning
  //////////////////////////////////////////////////////////////////////////////

  test("sel star pruning") {
    sc.sql("drop table if exists selstar")
    sc.sql("""create table selstar TBLPROPERTIES ("shark.cache" = "true") as
      select * from test""")
    expectSql("select * from selstar where val='val_487'","487	val_487")
  }

  test("map pruning with functions in between clause") {
    sc.sql("drop table if exists mapsplitfunc")
    sc.sql("drop table if exists mapsplitfunc_cached")
    sc.sql("create table mapsplitfunc(k bigint, v string)")
    sc.sql("""load data local inpath '${hiveconf:shark.test.data.path}/kv1.txt'
      OVERWRITE INTO TABLE mapsplitfunc""")
    sc.sql("create table mapsplitfunc_cached as select * from mapsplitfunc")
    expectSql("""select count(*) from mapsplitfunc_cached
      where month(from_unixtime(k)) between "1" and "12" """, Array[String]("500"))
    expectSql("""select count(*) from mapsplitfunc_cached
      where year(from_unixtime(k)) between "2013" and "2014" """, Array[String]("0"))
  }

  //////////////////////////////////////////////////////////////////////////////
  // SharkContext APIs (e.g. sql2rdd, sql)
  //////////////////////////////////////////////////////////////////////////////

  test("cached table in different new database") {
    sc.sql("drop table if exists selstar")
    sc.sql("""create table selstar TBLPROPERTIES ("shark.cache" = "true") as
      select * from default.test """)
    sc.sql("use seconddb")
    sc.sql("drop table if exists selstar")
    sc.sql("""create table selstar TBLPROPERTIES ("shark.cache" = "true") as
      select * from default.test where key != 'val_487' """)

    sc.sql("use default")
    expectSql("select * from selstar where val='val_487'","487	val_487")

    assert(SharkEnv.memoryMetadataManager.containsTable(DEFAULT_DB_NAME, "selstar"))
    assert(SharkEnv.memoryMetadataManager.containsTable("seconddb", "selstar"))

  }

  //////////////////////////////////////////////////////////////////////////////
  // various data types
  //////////////////////////////////////////////////////////////////////////////
  
  test("boolean data type") {
    sc.sql("drop table if exists checkboolean")
    sc.sql("""create table checkboolean TBLPROPERTIES ("shark.cache" = "true") as
      select key, val, true as flag from test where key < "300" """)
    sc.sql("""insert into table checkboolean
      select key, val, false as flag from test where key > "300" """)
    expectSql("select flag, count(*) from checkboolean group by flag order by flag asc",
      Array[String]("false\t208", "true\t292"))
  }

  test("byte data type") {
    sc.sql("drop table if exists checkbyte")
    sc.sql("drop table if exists checkbyte_cached")
    sc.sql("""create table checkbyte (key string, val string, flag tinyint) """)
    sc.sql("""insert into table checkbyte
      select key, val, 1 from test where key < "300" """)
    sc.sql("""insert into table checkbyte
      select key, val, 0 from test where key > "300" """)
    sc.sql("""create table checkbyte_cached as select * from checkbyte""")
    expectSql("select flag, count(*) from checkbyte_cached group by flag order by flag asc",
      Array[String]("0\t208", "1\t292"))
  }
    
  test("binary data type") {

    sc.sql("drop table if exists checkbinary")
    sc.sql("drop table if exists checkbinary_cached")
    sc.sql("""create table checkbinary (key string, flag binary) """)
    sc.sql("""insert into table checkbinary
      select key, cast(val as binary) as flag from test where key < "300" """)
    sc.sql("""insert into table checkbinary
      select key, cast(val as binary) as flag from test where key > "300" """)
    sc.sql("create table checkbinary_cached as select key, flag from checkbinary")
    expectSql("select cast(flag as string) as f from checkbinary_cached order by f asc limit 2",
      Array[String]("val_0", "val_0"))
  }
      
  test("short data type") {
    sc.sql("drop table if exists checkshort")
    sc.sql("drop table if exists checkshort_cached")
    sc.sql("""create table checkshort (key string, val string, flag smallint) """)
    sc.sql("""insert into table checkshort
      select key, val, 23 as flag from test where key < "300" """)
    sc.sql("""insert into table checkshort
      select key, val, 36 as flag from test where key > "300" """)
    sc.sql("create table checkshort_cached as select key, val, flag from checkshort")
    expectSql("select flag, count(*) from checkshort_cached group by flag order by flag asc",
      Array[String]("23\t292", "36\t208"))
  }

  //////////////////////////////////////////////////////////////////////////////
  // SharkContext APIs (e.g. sql2rdd, sql)
  //////////////////////////////////////////////////////////////////////////////

  test("sql max number of rows returned") {
    assert(sc.runSql("select * from test").results.size === 500)
    assert(sc.runSql("select * from test", 100).results.size === 100)
  }

  test("sql2rdd") {
    var rdd = sc.sql2rdd("select * from test")
    assert(rdd.count === 500)
    rdd = sc.sql2rdd("select * from test_cached")
    assert(rdd.count === 500)
    val collected = rdd.map(r => r.getInt(0)).collect().sortWith(_ < _)
    assert(collected(0) === 0)
    assert(collected(499) === 498)
    assert(collected.size === 500)
  }

  test("null values in sql2rdd") {
    val nullsRdd = sc.sql2rdd("select * from test_null where key is null")
    val nulls = nullsRdd.map(r => r.getInt(0)).collect()
    assert(nulls(0) === null)
    assert(nulls.size === 10)
  }

  test("sql exception") {
    val e = intercept[QueryExecutionException] { sc.runSql("asdfasdfasdfasdf") }
    e.getMessage.contains("semantic")
  }

  test("sql2rdd exception") {
    val e = intercept[QueryExecutionException] { sc.sql2rdd("asdfasdfasdfasdf") }
    e.getMessage.contains("semantic")
  }

  //////////////////////////////////////////////////////////////////////////////
  // Default cache mode is CacheType.MEMORY (unified view)
  //////////////////////////////////////////////////////////////////////////////
  test ("Table created by CREATE TABLE, with table properties, is CacheType.MEMORY by default") {
    sc.runSql("drop table if exists test_unify_creation")
    sc.runSql("""create table test_unify_creation (key int, val string) 
      tblproperties('shark.cache'='true')""")
    val table = sharkMetastore.getTable(DEFAULT_DB_NAME, "test_unify_creation").get
    assert(table.cacheMode == CacheType.MEMORY)
    sc.runSql("drop table if exists test_unify_creation")
  }

  test ("Table created by CREATE TABLE, with '_cached', is CacheType.MEMORY_ONLY by default") {
    sc.runSql("drop table if exists test_unify_creation_cached")
    sc.runSql("create table test_unify_creation_cached(key int, val string)")
    val table = sharkMetastore.getTable(DEFAULT_DB_NAME, "test_unify_creation_cached").get
    assert(table.cacheMode == CacheType.MEMORY_ONLY)
    sc.runSql("drop table if exists test_unify_creation_cached")
  }

  test ("Table created by CTAS, with table properties, is CacheType.MEMORY by default") {
    sc.runSql("drop table if exists test_unify_ctas")
    sc.runSql("""create table test_unify_ctas tblproperties('shark.cache' = 'true')
      as select  * from test""")
    val table = sharkMetastore.getTable(DEFAULT_DB_NAME, "test_unify_ctas").get
    assert(table.cacheMode == CacheType.MEMORY)
    expectSql("select count(*) from test_unify_ctas", "500")
    sc.runSql("drop table if exists test_unify_ctas")
  }

  test ("Table created by CTAS, with '_cached', is CacheType.MEMORY_ONLY by default") {
    sc.runSql("drop table if exists test_unify_ctas_cached")
    sc.runSql("create table test_unify_ctas_cached as select  * from test")
    val table = sharkMetastore.getTable(DEFAULT_DB_NAME, "test_unify_ctas_cached").get
    assert(table.cacheMode == CacheType.MEMORY_ONLY)
    expectSql("select count(*) from test_unify_ctas_cached", "500")
    sc.runSql("drop table if exists test_unify_ctas_cached")
  }

  test ("CREATE TABLE when 'shark.cache' is CacheType.MEMORY_ONLY") {
    sc.runSql("drop table if exists test_non_unify_creation")
    sc.runSql("""create table test_non_unify_creation(key int, val string) 
      tblproperties('shark.cache' = 'memory_only')""")
    val table = sharkMetastore.getTable(DEFAULT_DB_NAME, "test_non_unify_creation").get
    assert(table.cacheMode == CacheType.MEMORY_ONLY)
    sc.runSql("drop table if exists test_non_unify_creation")
  }

  test ("CTAS when 'shark.cache' is CacheType.MEMORY_ONLY") {
    sc.runSql("drop table if exists test_non_unify_ctas")
    sc.runSql("""create table test_non_unify_ctas tblproperties
      ('shark.cache' = 'memory_only') as select  * from test""")
    val table = sharkMetastore.getTable(DEFAULT_DB_NAME, "test_non_unify_ctas").get
    assert(table.cacheMode == CacheType.MEMORY_ONLY)
    sc.runSql("drop table if exists test_non_unify_ctas")
  }

  //////////////////////////////////////////////////////////////////////////////
  // LOAD for tables cached in memory and stored on disk (unified view)
  //////////////////////////////////////////////////////////////////////////////
  test ("LOAD INTO unified view") {
    sc.runSql("drop table if exists unified_view_cached")
    sc.runSql(
      """create table unified_view_cached (key int, value string)
        |tblproperties("shark.cache" = "memory")
      """.stripMargin)
    sc.runSql("load data local inpath '%s' into table unified_view_cached".format(KV1_TXT_PATH))
    expectUnifiedKVTable("unified_view_cached")
    expectSql("select count(*) from unified_view_cached", "500")
    sc.runSql("drop table if exists unified_view_cached")
  }

  test ("LOAD OVERWRITE unified view") {
    sc.runSql("drop table if exists unified_overwrite_cached")
    sc.runSql("create table unified_overwrite_cached (key int, value string)" +
      "tblproperties(\"shark.cache\" = \"memory\")")
    sc.runSql("load data local inpath '%s' into table unified_overwrite_cached".
      format("${hiveconf:shark.test.data.path}/kv3.txt"))
    expectSql("select count(*) from unified_overwrite_cached", "25")
    sc.runSql("load data local inpath '%s' overwrite into table unified_overwrite_cached".
      format(KV1_TXT_PATH))
    // Make sure the cached contents matches the disk contents.
    expectUnifiedKVTable("unified_overwrite_cached")
    expectSql("select count(*) from unified_overwrite_cached", "500")
    sc.runSql("drop table if exists unified_overwrite_cached")
  }

  test ("LOAD INTO partitioned unified view") {
    sc.runSql("drop table if exists unified_view_part_cached")
    sc.runSql("""create table unified_view_part_cached (key int, value string)
      partitioned by (keypart int) tblproperties("shark.cache" = "memory")""")
    sc.runSql("""load data local inpath '%s' into table unified_view_part_cached
      partition(keypart = 1)""".format(KV1_TXT_PATH))
    expectUnifiedKVTable("unified_view_part_cached", Some(Map("keypart" -> "1")))
    expectSql("select count(*) from unified_view_part_cached", "500")
    sc.runSql("drop table if exists unified_view_part_cached")
  }

  test ("LOAD OVERWRITE partitioned unified view") {
    sc.runSql("drop table if exists unified_overwrite_part_cached")
    sc.runSql("""create table unified_overwrite_part_cached (key int, value string)
      partitioned by (keypart int) tblproperties("shark.cache" = "memory")""")
    sc.runSql("""load data local inpath '%s' overwrite into table unified_overwrite_part_cached
      partition(keypart = 1)""".format(KV1_TXT_PATH))
    expectUnifiedKVTable("unified_overwrite_part_cached", Some(Map("keypart" -> "1")))
    expectSql("select count(*) from unified_overwrite_part_cached", "500")
    sc.runSql("drop table if exists unified_overwrite_part_cached")
  }

  //////////////////////////////////////////////////////////////////////////////
  // INSERT for tables cached in memory and stored on disk (unified view)
  //////////////////////////////////////////////////////////////////////////////
  test ("INSERT INTO unified view") {
    sc.runSql("drop table if exists unified_view_cached")
    sc.runSql("create table unified_view_cached tblproperties('shark.cache'='memory') " +
      "as select * from test_cached")
    sc.runSql("insert into table unified_view_cached select * from test_cached")
    expectUnifiedKVTable("unified_view_cached")
    expectSql("select count(*) from unified_view_cached", "1000")
    sc.runSql("drop table if exists unified_view_cached")
  }

  test ("INSERT OVERWRITE unified view") {
    sc.runSql("drop table if exists unified_overwrite_cached")
    sc.runSql("create table unified_overwrite_cached tblproperties('shark.cache'='memory')" +
      "as select * from test")
    sc.runSql("insert overwrite table unified_overwrite_cached select * from test_cached")
    expectUnifiedKVTable("unified_overwrite_cached")
    expectSql("select count(*) from unified_overwrite_cached", "500")
    sc.runSql("drop table if exists unified_overwrite_cached")
  }

  test ("INSERT INTO partitioned unified view") {
    sc.runSql("drop table if exists unified_view_part_cached")
    sc.runSql("""create table unified_view_part_cached (key int, value string)
                 partitioned by (keypart int)
                 tblproperties('shark.cache'='memory')""")
    sc.runSql("""insert into table unified_view_part_cached partition (keypart = 1) 
      select * from test_cached""")
    expectUnifiedKVTable("unified_view_part_cached", Some(Map("keypart" -> "1")))
    expectSql("select count(*) from unified_view_part_cached where keypart = 1", "500")
    sc.runSql("drop table if exists unified_view_part_cached")
  }

  test ("INSERT OVERWRITE partitioned unified view") {
    sc.runSql("drop table if exists unified_overwrite_part_cached")
    sc.runSql("""create table unified_overwrite_part_cached (key int, value string)
                 partitioned by (keypart int) tblproperties('shark.cache'='memory')""")
    sc.runSql("""insert overwrite table unified_overwrite_part_cached partition (keypart = 1) 
      select * from test_cached""")
    expectUnifiedKVTable("unified_overwrite_part_cached", Some(Map("keypart" -> "1")))
    expectSql("select count(*) from unified_overwrite_part_cached", "500")
    sc.runSql("drop table if exists unified_overwrite_part_cached")
  }

  //////////////////////////////////////////////////////////////////////////////
  // CACHE and ALTER TABLE commands
  //////////////////////////////////////////////////////////////////////////////
  test ("ALTER TABLE caches non-partitioned table if 'shark.cache' is set to true") {
    sc.runSql("drop table if exists unified_load")
    sc.runSql("create table unified_load as select * from test")
    sc.runSql("alter table unified_load set tblproperties('shark.cache' = 'true')")
    expectUnifiedKVTable("unified_load")
    sc.runSql("drop table if exists unified_load")
  }

  test ("ALTER TABLE caches partitioned table if 'shark.cache' is set to true") {
    sc.runSql("drop table if exists unified_part_load")
    sc.runSql("create table unified_part_load (key int, value string) partitioned by (keypart int)")
    sc.runSql("insert into table unified_part_load partition (keypart=1) select * from test_cached")
    sc.runSql("alter table unified_part_load set tblproperties('shark.cache' = 'true')")
    expectUnifiedKVTable("unified_part_load", Some(Map("keypart" -> "1")))
    sc.runSql("drop table if exists unified_part_load")
  }

  test ("ALTER TABLE uncaches non-partitioned table if 'shark.cache' is set to false") {
    sc.runSql("drop table if exists unified_load")
    sc.runSql("create table unified_load as select * from test")
    sc.runSql("alter table unified_load set tblproperties('shark.cache' = 'false')")
    assert(!sharkMetastore.containsTable(DEFAULT_DB_NAME, "unified_load"))
    expectSql("select count(*) from unified_load", "500")
    sc.runSql("drop table if exists unified_load")
  }

  test ("ALTER TABLE uncaches partitioned table if 'shark.cache' is set to false") {
    sc.runSql("drop table if exists unified_part_load")
    sc.runSql("create table unified_part_load (key int, value string) partitioned by (keypart int)")
    sc.runSql("insert into table unified_part_load partition (keypart=1) select * from test_cached")
    sc.runSql("alter table unified_part_load set tblproperties('shark.cache' = 'false')")
    assert(!sharkMetastore.containsTable(DEFAULT_DB_NAME, "unified_part_load"))
    expectSql("select count(*) from unified_part_load", "500")
    sc.runSql("drop table if exists unified_part_load")
  }

  test ("UNCACHE behaves like ALTER TABLE SET TBLPROPERTIES ...") {
    sc.runSql("drop table if exists unified_load")
    sc.runSql("create table unified_load as select * from test")
    sc.runSql("cache unified_load")
    // Double check the table properties.
    val tableName = "unified_load"
    val hiveTable = Hive.get().getTable(DEFAULT_DB_NAME, tableName)
    assert(hiveTable.getProperty("shark.cache") == "MEMORY")
    // Check that the cache and disk contents are synchronized.
    expectUnifiedKVTable(tableName)
    sc.runSql("drop table if exists unified_load")
  }

  test ("CACHE behaves like ALTER TABLE SET TBLPROPERTIES ...") {
    sc.runSql("drop table if exists unified_load")
    sc.runSql("create table unified_load as select * from test")
    sc.runSql("cache unified_load")
    // Double check the table properties.
    val tableName = "unified_load"
    val hiveTable = Hive.get().getTable(DEFAULT_DB_NAME, tableName)
    assert(hiveTable.getProperty("shark.cache") == "MEMORY")
    // Check that the cache and disk contents are synchronized.
    expectUnifiedKVTable(tableName)
    sc.runSql("drop table if exists unified_load")
  }

  //////////////////////////////////////////////////////////////////////////////
  // Cached table persistence
  //////////////////////////////////////////////////////////////////////////////
  ignore ("Cached tables persist across Shark metastore shutdowns.") {
    val globalCachedTableNames = Seq("test_cached", "test_null_cached", "clicks_cached",
      "users_cached")

    // Number of rows for each cached table.
    val cachedTableCounts = new Array[String](globalCachedTableNames.size)
    for ((tableName, i) <- globalCachedTableNames.zipWithIndex) {
      val hiveTable = Hive.get().getTable(DEFAULT_DB_NAME, tableName)
      val cachedCount = sc.sql("select count(*) from %s".format(tableName))(0)
      cachedTableCounts(i) = cachedCount
    }
    sharkMetastore.shutdown()
    for ((tableName, i) <- globalCachedTableNames.zipWithIndex) {
      val hiveTable = Hive.get().getTable(DEFAULT_DB_NAME, tableName)
      // Check that the number of rows from the table on disk remains the same.
      val onDiskCount = sc.sql("select count(*) from %s".format(tableName))(0)
      val cachedCount = cachedTableCounts(i)
      assert(onDiskCount == cachedCount, """Num rows for %s differ across Shark metastore restart. 
        (rows cached = %s, rows on disk = %s)""".format(tableName, cachedCount, onDiskCount))
      // Check that we're able to materialize a row - i.e., make sure that table scan operator
      // doesn't try to use a ColumnarSerDe when scanning contents on disk (for our test tables,
      // LazySimpleSerDes should be used).
      sc.sql("select * from %s limit 1".format(tableName))
    }
    // Finally, reload all tables.
    SharkRunner.loadTables()
  }

  //////////////////////////////////////////////////////////////////////////////
  // Window Function Support
  //////////////////////////////////////////////////////////////////////////////
  test("window function support") {
    expectSql("select id,name,count(id) over (partition by name) from users",
      Array[String]("1\tA\t2", "3\tA\t2", "2\tB\t1"))
    expectSql("select id,name,sum(id) over(partition by name order by id) from users",
      Array[String]("1\tA\t1", "3\tA\t4", "2\tB\t2"))
    expectSql("select id,name,sum(id) over(partition by name order by id rows between " +
      "unbounded preceding and current row) from users",
      Array[String]("1\tA\t1", "3\tA\t4", "2\tB\t2"))
    expectSql("select id,name,sum(id) over(partition by name order by id rows between " +
      "current row and unbounded following) from users",
      Array[String]("1\tA\t4", "3\tA\t3", "2\tB\t2"))
    expectSql("select id,name,sum(id) over(partition by name order by id rows between " +
      "unbounded preceding and unbounded following) from users",
      Array[String]("1\tA\t4", "3\tA\t4", "2\tB\t2"))
    expectSql("select id,name,lead(id) over(partition by name order by id) from users",
      Array[String]("1\tA\t3", "3\tA\tnull", "2\tB\tnull"))
    expectSql("select id,name,lag(id) over(partition by name order by id) from users",
      Array[String]("1\tA\tnull", "3\tA\t1", "2\tB\tnull"))
    expectSql("select id, name, sum(id) over w1 as sum_id, max(id) over w1 as max_id from users" +
      " window w1 as (partition by name)",
      Array[String]("2\tB\t2\t2","1\tA\t4\t3","3\tA\t4\t3"))
  }

  //////////////////////////////////////////////////////////////////////////////
  // Table Generating Functions (TGFs)
  //////////////////////////////////////////////////////////////////////////////

  test("Simple TGFs") {
    expectSql("generate shark.TestTGF1(test, 15)", Array(15,15,15,17,19).map(_.toString).toArray)
  }

  test("Saving simple TGFs") {
    sc.sql("drop table if exists TGFTestTable")
    sc.runSql("generate shark.TestTGF1(test, 15) as TGFTestTable")
    expectSql("select * from TGFTestTable", Array(15,15,15,17,19).map(_.toString).toArray)
    sc.sql("drop table if exists TGFTestTable")
  }

  test("Advanced TGFs") {
    expectSql("generate shark.TestTGF2(test, 25)", Array(25,25,25,27,29).map(_.toString).toArray)
  }

  test("Saving advanced TGFs") {
    sc.sql("drop table if exists TGFTestTable2")
    sc.runSql("generate shark.TestTGF2(test, 25) as TGFTestTable2")
    expectSql("select * from TGFTestTable2", Array(25,25,25,27,29).map(_.toString).toArray)
    sc.sql("drop table if exists TGFTestTable2")
  }
}

object TestTGF1 {
  @Schema(spec = "values int")
  def apply(test: RDD[(Int, String)], integer: Int) = {
    test.map{ case Tuple2(k, v) => Tuple1(k + integer) }.filter{ case Tuple1(v) => v < 20 }
  }
}

object TestTGF2 {
  def apply(sc: SharkContext, test: RDD[(Int, String)], integer: Int) = {
    val rdd = test.map{ case Tuple2(k, v) => Seq(k + integer) }.filter{ case Seq(v) => v < 30 }
    RDDSchema(rdd.asInstanceOf[RDD[Seq[_]]], "myvalues int")
  }
}
