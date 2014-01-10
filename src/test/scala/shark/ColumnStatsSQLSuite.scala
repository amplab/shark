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

import org.apache.hadoop.io.BytesWritable

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME

import org.apache.spark.rdd.RDD

import shark.memstore2.MemoryMetadataManager


class ColumnStatsSQLSuite extends FunSuite with BeforeAndAfterAll {

  val sc: SharkContext = SharkRunner.init()
  val sharkMetastore = SharkEnv.memoryMetadataManager

  // import expectSql() shortcut methods
  import shark.SharkRunner._

  override def beforeAll() {
  	sc.runSql("drop table if exists srcpart_cached")
    sc.runSql("create table srcpart_cached(key int, val string) partitioned by (keypart int)")
    sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/kv1.txt'
      into table srcpart_cached partition (keypart = 1)""")
  }

  override def afterAll() {
  	sc.runSql("drop table if exists srcpart_cached")
  }

  test("Hive partition stats are tracked") {
    val tableOpt = sharkMetastore.getPartitionedTable(DEFAULT_DATABASE_NAME, "srcpart_cached")
    assert(tableOpt.isDefined)
    val partitionToStatsOpt = tableOpt.get.getStats("keypart=1")
    assert(partitionToStatsOpt.isDefined)
    val partitionToStats = partitionToStatsOpt.get
    // The 'kv1.txt' file loaded into 'keypart=1' in beforeAll() has 2 partitions.
    assert(partitionToStats.size == 2)
  }

  test("Hive partition stats are tracked after LOADs and INSERTs") {
  	// Load more data into srcpart_cached
    sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/kv1.txt'
      into table srcpart_cached partition (keypart = 1)""")
    val tableOpt = sharkMetastore.getPartitionedTable(DEFAULT_DATABASE_NAME, "srcpart_cached")
    assert(tableOpt.isDefined)
    var partitionToStatsOpt = tableOpt.get.getStats("keypart=1")
    assert(partitionToStatsOpt.isDefined)
    var partitionToStats = partitionToStatsOpt.get
    // The 'kv1.txt' file loaded into 'keypart=1' has 2 partitions. We've loaded it twice at this
    // point.
    assert(partitionToStats.size == 4)

    // Append using INSERT command
    sc.runSql("insert into table srcpart_cached partition(keypart = 1) select * from test")
    partitionToStatsOpt = tableOpt.get.getStats("keypart=1")
    assert(partitionToStatsOpt.isDefined)
    partitionToStats = partitionToStatsOpt.get
    assert(partitionToStats.size == 6)

    // INSERT OVERWRITE should overrwritie old table stats. This also restores srcpart_cached
    // to contents contained before this test.
    sc.runSql("""insert overwrite table srcpart_cached partition(keypart = 1)
      select * from test""")
    partitionToStatsOpt = tableOpt.get.getStats("keypart=1")
    assert(partitionToStatsOpt.isDefined)
    partitionToStats = partitionToStatsOpt.get
    assert(partitionToStats.size == 2)
  }

  //////////////////////////////////////////////////////////////////////////////
  // End-to-end sanity checks
  //////////////////////////////////////////////////////////////////////////////
  test("column pruning filters") {
    expectSql("select count(*) from test_cached where key > -1", "500")
  }

  test("column pruning group by") {
    expectSql("select key, count(*) from test_cached group by key order by key limit 1", "0\t3")
  }

  test("column pruning group by with single filter") {
    expectSql("select key, count(*) from test_cached where val='val_484' group by key", "484\t1")
  }

  test("column pruning aggregate function") {
    expectSql("select val, sum(key) from test_cached group by val order by val desc limit 1",
      "val_98\t196")
  }

  test("column pruning filters for a Hive partition") {
    expectSql("select count(*) from srcpart_cached where key > -1", "500")
    expectSql("select count(*) from srcpart_cached where key > -1 and keypart = 1", "500")
  }

  test("column pruning group by for a Hive partition") {
    expectSql("select key, count(*) from srcpart_cached group by key order by key limit 1", "0\t3")
  }

  test("column pruning group by with single filter for a Hive partition") {
    expectSql("select key, count(*) from srcpart_cached where val='val_484' group by key", "484\t1")
  }

  test("column pruning aggregate function for a Hive partition") {
    expectSql("select val, sum(key) from srcpart_cached group by val order by val desc limit 1",
      "val_98\t196")
  }

}
