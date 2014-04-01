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

import java.util.{HashMap => JavaHashMap}

import scala.collection.JavaConversions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel

import shark.api.QueryExecutionException
import shark.memstore2.{OffHeapStorageClient, CacheType, MemoryMetadataManager, PartitionedMemoryTable}
// import expectSql() shortcut methods
import shark.SharkRunner._


class TachyonSQLSuite extends FunSuite with BeforeAndAfterAll {

  val DEFAULT_DB_NAME = DEFAULT_DATABASE_NAME
  val KV1_TXT_PATH = "${hiveconf:shark.test.data.path}/kv1.txt"

  var sc: SharkContext = SharkRunner.init()
  var sharkMetastore: MemoryMetadataManager = SharkEnv.memoryMetadataManager

  // Determine if Tachyon enabled at runtime.
  val isTachyonEnabled = sys.env.contains("TACHYON_MASTER")

  override def beforeAll() {
    if (isTachyonEnabled) {
      sc.runSql("create table test_tachyon as select * from test")
    }
  }

  override def afterAll() {
    if (isTachyonEnabled) {
      sc.runSql("drop table test_tachyon")
    }
  }

  private def isTachyonTable(
      dbName: String,
      tableName: String,
      hivePartitionKeyOpt: Option[String] = None): Boolean = {
    val tableKey = MemoryMetadataManager.makeTableKey(dbName, tableName)
    OffHeapStorageClient.client.tablePartitionExists(tableKey, hivePartitionKeyOpt)
  }

  private def createPartitionedTachyonTable(tableName: String, numPartitionsToCreate: Int) {
    sc.runSql("drop table if exists %s".format(tableName))
    sc.runSql("""
      create table %s(key int, value string)
        partitioned by (keypart int)
        tblproperties('shark.cache' = 'tachyon')
      """.format(tableName))
    var partitionNum = 1
    while (partitionNum <= numPartitionsToCreate) {
      sc.runSql("""insert into table %s partition(keypart = %d)
        select * from test_tachyon""".format(tableName, partitionNum))
      partitionNum += 1
    }
    assert(isTachyonTable(DEFAULT_DB_NAME, tableName))
  }

  if (isTachyonEnabled) {
    //////////////////////////////////////////////////////////////////////////////
    // basic SQL
    //////////////////////////////////////////////////////////////////////////////
    test("count") {
      expectSql("select count(*) from test_tachyon", "500")
    }

    test("filter") {
     expectSql("select * from test_tachyon where key=100 or key=497",
      Array("100\tval_100", "100\tval_100", "497\tval_497"))
    }

    test("count distinct") {
      sc.runSql("set mapred.reduce.tasks=3")
      expectSql("select count(distinct key) from test_tachyon", "309")
      expectSql(
        """|SELECT substr(key,1,1), count(DISTINCT substr(val,5)) from test_tachyon
           |GROUP BY substr(key,1,1)""".stripMargin,
        Array("0\t1", "1\t71", "2\t69", "3\t62", "4\t74", "5\t6", "6\t5", "7\t6", "8\t8", "9\t7"))
    }

    test("count bigint") {
      sc.runSql("drop table if exists test_bigint")
      sc.runSql("create table test_bigint (key bigint, val string)")
      sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/kv1.txt'
        OVERWRITE INTO TABLE test_bigint""")
      sc.runSql("drop table if exists test_bigint_tachyon")
      sc.runSql("create table test_bigint_tachyon as select * from test_bigint")
      expectSql("select val, count(*) from test_bigint_tachyon where key=484 group by val",
        "val_484\t1")

      sc.runSql("drop table if exists test_bigint_tachyon")
    }

    test("limit") {
      assert(sc.runSql("select * from test_tachyon limit 10").results.length === 10)
      assert(sc.runSql("select * from test_tachyon limit 501").results.length === 500)
      sc.runSql("drop table if exists test_limit0_tachyon")
      assert(sc.runSql("select * from test_tachyon limit 0").results.length === 0)
      assert(sc.runSql("create table test_limit0_tachyon as select * from test_tachyon limit 0")
        .results.length === 0)
      assert(sc.runSql("select * from test_limit0_tachyon limit 0").results.length === 0)
      assert(sc.runSql("select * from test_limit0_tachyon limit 1").results.length === 0)

      sc.runSql("drop table if exists test_limit0_tachyon")
    }

    //////////////////////////////////////////////////////////////////////////////
    // cache DDL
    //////////////////////////////////////////////////////////////////////////////
    test("Use regular CREATE TABLE and '_tachyon' suffix to create Tachyon table") {
      sc.runSql("drop table if exists empty_table_tachyon")
      sc.runSql("create table empty_table_tachyon(key string, value string)")
      assert(isTachyonTable(DEFAULT_DB_NAME, "empty_table_tachyon"))

      sc.runSql("drop table if exists empty_table_tachyon")
    }

    test("Use regular CREATE TABLE and table properties to create Tachyon table") {
      sc.runSql("drop table if exists empty_table_tachyon_tbl_props")
      sc.runSql("""create table empty_table_tachyon_tbl_props(key string, value string)
        TBLPROPERTIES('shark.cache' = 'tachyon')""")
      assert(isTachyonTable(DEFAULT_DB_NAME, "empty_table_tachyon_tbl_props"))

      sc.runSql("drop table if exists empty_table_tachyon_tbl_props")
    }

    test("Insert into empty Tachyon table") {
      sc.runSql("drop table if exists new_table_tachyon")
      sc.runSql("create table new_table_tachyon(key string, value string)")
      sc.runSql("insert into table new_table_tachyon select * from test where key > -1 limit 499")
      expectSql("select count(*) from new_table_tachyon", "499")

      sc.runSql("drop table if exists new_table_tachyon")
    }

    test("rename Tachyon table") {
      sc.runSql("drop table if exists test_oldname_tachyon")
      sc.runSql("drop table if exists test_rename")
      sc.runSql("create table test_oldname_tachyon as select * from test")
      sc.runSql("alter table test_oldname_tachyon rename to test_rename")

      assert(!isTachyonTable(DEFAULT_DB_NAME, "test_oldname_tachyon"))
      assert(isTachyonTable(DEFAULT_DB_NAME, "test_rename"))

      expectSql("select count(*) from test_rename", "500")

      sc.runSql("drop table if exists test_rename")
    }

    test("insert into tachyon tables") {
      sc.runSql("drop table if exists test1_tachyon")
      sc.runSql("create table test1_tachyon as select * from test")
      expectSql("select count(*) from test1_tachyon", "500")
      sc.runSql("insert into table test1_tachyon select * from test where key > -1 limit 499")
      expectSql("select count(*) from test1_tachyon", "999")

      sc.runSql("drop table if exists test1_tachyon")
    }

    test("insert overwrite") {
      sc.runSql("drop table if exists test2_tachyon")
      sc.runSql("create table test2_tachyon as select * from test")
      expectSql("select count(*) from test2_tachyon", "500")
      sc.runSql("insert overwrite table test2_tachyon select * from test where key > -1 limit 499")
      expectSql("select count(*) from test2_tachyon", "499")

      sc.runSql("drop table if exists test2_tachyon")
    }

    test("error when attempting to update Tachyon table(s) using command with multiple INSERTs") {
      sc.runSql("drop table if exists multi_insert_test")
      sc.runSql("drop table if exists multi_insert_test_tachyon")
      sc.runSql("create table multi_insert_test as select * from test")
      sc.runSql("create table multi_insert_test_tachyon as select * from test")
      intercept[QueryExecutionException] {
        sc.runSql("""from test
          insert into table multi_insert_test select *
          insert into table multi_insert_test_tachyon select *""")
      }

      sc.runSql("drop table if exists multi_insert_test")
      sc.runSql("drop table if exists multi_insert_test_tachyon")
    }

    test("create Tachyon table with 'shark.cache' flag in table properties") {
      sc.runSql("drop table if exists ctas_tbl_props")
      sc.runSql("""create table ctas_tbl_props TBLPROPERTIES ('shark.cache'='tachyon') as
        select * from test""")
      assert(isTachyonTable(DEFAULT_DB_NAME, "ctas_tbl_props"))
      expectSql("select * from ctas_tbl_props where key=407", "407\tval_407")

      sc.runSql("drop table if exists ctas_tbl_props")
    }

    test("tachyon tables with complex types") {
      sc.runSql("drop table if exists test_complex_types")
      sc.runSql("drop table if exists test_complex_types_tachyon")
      sc.runSql("""CREATE TABLE test_complex_types (
        a STRING, b ARRAY<STRING>, c ARRAY<MAP<STRING,STRING>>, d MAP<STRING,ARRAY<STRING>>)""")
      sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/create_nested_type.txt'
        overwrite into table test_complex_types""")
      sc.runSql("""create table test_complex_types_tachyon TBLPROPERTIES ("shark.cache" = "tachyon") 
        as select * from test_complex_types""")

      assert(sc.sql("select a from test_complex_types_tachyon where a = 'a0'").head === "a0")

      assert(sc.sql("select b from test_complex_types_tachyon where a = 'a0'").head ===
        """["b00","b01"]""")

      assert(sc.sql("select c from test_complex_types_tachyon where a = 'a0'").head ===
        """[{"c001":"C001","c002":"C002"},{"c011":null,"c012":"C012"}]""")

      assert(sc.sql("select d from test_complex_types_tachyon where a = 'a0'").head ===
        """{"d01":["d011","d012"],"d02":["d021","d022"]}""")

      assert(isTachyonTable(DEFAULT_DB_NAME, "test_complex_types_tachyon"))

      sc.runSql("drop table if exists test_complex_types")
      sc.runSql("drop table if exists test_complex_types_tachyon")
    }

    test("disable caching in Tachyon by default") {
      sc.runSql("set shark.cache.flag.checkTableName=false")
      sc.runSql("drop table if exists should_not_be_in_tachyon")
      sc.runSql("create table should_not_be_in_tachyon as select * from test")
      expectSql("select key from should_not_be_in_tachyon where key = 407", "407")
      assert(!isTachyonTable(DEFAULT_DB_NAME, "should_not_be_in_tachyon"))

      sc.runSql("set shark.cache.flag.checkTableName=true")
      sc.runSql("drop table if exists should_not_be_in_tachyon")
    }

    test("tachyon table name should be case-insensitive") {
      sc.runSql("drop table if exists sharkTest5tachyon")
      sc.runSql("""create table sharkTest5tachyon TBLPROPERTIES ("shark.cache" = "tachyon") as
        select * from test""")
      expectSql("select val from sharktest5tachyon where key = 407", "val_407")
      assert(isTachyonTable(DEFAULT_DB_NAME, "sharkTest5tachyon"))

      sc.runSql("drop table if exists sharkTest5tachyon")
    }

    test("dropping tachyon tables should clean up RDDs") {
      sc.runSql("drop table if exists sharkTest5tachyon")
      sc.runSql("""create table sharkTest5tachyon TBLPROPERTIES ("shark.cache" = "tachyon") as
        select * from test""")
      sc.runSql("drop table sharkTest5tachyon")
      assert(!isTachyonTable(DEFAULT_DB_NAME, "sharkTest5tachyon"))
    }

    //////////////////////////////////////////////////////////////////////////////
    // Caching Hive-partititioned tables
    // Note: references to 'partition' for this section refer to a Hive-partition.
    //////////////////////////////////////////////////////////////////////////////
    test("Use regular CREATE TABLE and '_tachyon' suffix to create partitioned Tachyon table") {
      sc.runSql("drop table if exists empty_part_table_tachyon")
      sc.runSql("""create table empty_part_table_tachyon(key int, value string)
        partitioned by (keypart int)""")
      assert(isTachyonTable(DEFAULT_DB_NAME, "empty_part_table_tachyon"))

      sc.runSql("drop table if exists empty_part_table_tachyon")
    }

    test("Use regular CREATE TABLE and table properties to create partitioned Tachyon table") {
      sc.runSql("drop table if exists empty_part_table_tachyon_tbl_props")
      sc.runSql("""create table empty_part_table_tachyon_tbl_props(key int, value string)
        partitioned by (keypart int) tblproperties('shark.cache' = 'tachyon')""")
      assert(isTachyonTable(DEFAULT_DB_NAME, "empty_part_table_tachyon_tbl_props"))

      sc.runSql("drop table if exists empty_part_table_tachyon_tbl_props")
    }

    test("alter Tachyon table by adding a new partition") {
      sc.runSql("drop table if exists alter_part_tachyon")
      sc.runSql("""create table alter_part_tachyon(key int, value string)
        partitioned by (keypart int)""")
      sc.runSql("""alter table alter_part_tachyon add partition(keypart = 1)""")
      val tableName = "alter_part_tachyon"
      val partitionColumn = "keypart=1"
      assert(isTachyonTable(DEFAULT_DB_NAME, "alter_part_tachyon", Some(partitionColumn)))

      sc.runSql("drop table if exists alter_part_tachyon")
    }

    test("alter Tachyon table by dropping a partition") {
      sc.runSql("drop table if exists alter_drop_tachyon")
      sc.runSql("""create table alter_drop_tachyon(key int, value string)
        partitioned by (keypart int)""")
      sc.runSql("""alter table alter_drop_tachyon add partition(keypart = 1)""")

      val tableName = "alter_drop_tachyon"
      val partitionColumn = "keypart=1"
      assert(isTachyonTable(DEFAULT_DB_NAME, "alter_drop_tachyon", Some(partitionColumn)))
      sc.runSql("""alter table alter_drop_tachyon drop partition(keypart = 1)""")
      assert(!isTachyonTable(DEFAULT_DB_NAME, "alter_drop_tachyon", Some(partitionColumn)))

      sc.runSql("drop table if exists alter_drop_tachyon")
    }

    test("insert into a partition of a Tachyon table") {
      val tableName = "insert_part_tachyon"
      createPartitionedTachyonTable(
        tableName,
        numPartitionsToCreate = 1)
      expectSql("select value from insert_part_tachyon where key = 407 and keypart = 1", "val_407")

      sc.runSql("drop table if exists insert_part_tachyon")
    }

    test("insert overwrite a partition of a Tachyon table") {
      val tableName = "insert_over_part_tachyon"
      createPartitionedTachyonTable(
        tableName,
        numPartitionsToCreate = 1)
      expectSql("""select value from insert_over_part_tachyon
        where key = 407 and keypart = 1""", "val_407")
      sc.runSql("""insert overwrite table insert_over_part_tachyon partition(keypart = 1)
        select key, -1 from test""")
      expectSql("select value from insert_over_part_tachyon where key = 407 and keypart = 1", "-1")

      sc.runSql("drop table if exists insert_over_part_tachyon")
    }

    test("scan partitioned Tachyon table that's empty") {
      sc.runSql("drop table if exists empty_part_table_tachyon")
      sc.runSql("""create table empty_part_table_tachyon(key int, value string)
        partitioned by (keypart int)""")
      expectSql("select count(*) from empty_part_table_tachyon", "0")

      sc.runSql("drop table if exists empty_part_table_tachyon")
    }

    test("scan partitioned Tachyon table that has a single partition") {
      val tableName = "scan_single_part_tachyon"
      createPartitionedTachyonTable(
        tableName,
        numPartitionsToCreate = 1)
      expectSql("select * from scan_single_part_tachyon where key = 407", "407\tval_407\t1")

      sc.runSql("drop table if exists scan_single_part_tachyon")
    }

    test("scan partitioned Tachyon table that has multiple partitions") {
      val tableName = "scan_mult_part_tachyon"
      createPartitionedTachyonTable(
        tableName,
        numPartitionsToCreate = 3)
      expectSql("select * from scan_mult_part_tachyon where key = 407 order by keypart",
        Array("407\tval_407\t1", "407\tval_407\t2", "407\tval_407\t3"))

      sc.runSql("drop table if exists scan_mult_part_tachyon")
    }

    test("drop/unpersist partitioned Tachyon table that has multiple partitions") {
      val tableName = "drop_mult_part_tachyon"
      createPartitionedTachyonTable(
        tableName,
        numPartitionsToCreate = 3)
      expectSql("select count(1) from drop_mult_part_tachyon", "1500")
      sc.runSql("drop table drop_mult_part_tachyon ")
      assert(!isTachyonTable(DEFAULT_DB_NAME, tableName))

      sc.runSql("drop table if exists drop_mult_part_tachyon")
    }

    /////////////////////////////////////////////////////////////////////////////
    // LOAD for Tachyon tables
    //////////////////////////////////////////////////////////////////////////////
    test ("LOAD INTO a Tachyon table") {
      sc.runSql("drop table if exists load_into_tachyon")
      sc.runSql("create table load_into_tachyon (key int, value string)")
      sc.runSql("load data local inpath '%s' into table load_into_tachyon".format(KV1_TXT_PATH))
      expectSql("select count(*) from load_into_tachyon", "500")

      sc.runSql("drop table if exists load_into_tachyon")
    }

    test ("LOAD OVERWRITE a Tachyon table") {
      sc.runSql("drop table if exists load_overwrite_tachyon")
      sc.runSql("create table load_overwrite_tachyon (key int, value string)")
      sc.runSql("load data local inpath '%s' into table load_overwrite_tachyon".
        format("${hiveconf:shark.test.data.path}/kv3.txt"))
      expectSql("select count(*) from load_overwrite_tachyon", "25")
      sc.runSql("load data local inpath '%s' overwrite into table load_overwrite_tachyon".
        format(KV1_TXT_PATH))
      expectSql("select count(*) from load_overwrite_tachyon", "500")
      sc.runSql("drop table if exists load_overwrite_tachyon")
    }

    test ("LOAD INTO a partitioned Tachyon table") {
      sc.runSql("drop table if exists load_into_part_tachyon")
      sc.runSql("""create table load_into_part_tachyon (key int, value string)
        partitioned by (keypart int)""")
      sc.runSql("""load data local inpath '%s' into table load_into_part_tachyon
        partition(keypart = 1)""".format(KV1_TXT_PATH))
      expectSql("select count(*) from load_into_part_tachyon", "500")
      sc.runSql("drop table if exists load_into_part_tachyon")
    }

    test ("LOAD OVERWRITE a partitioned Tachyon table") {
      sc.runSql("drop table if exists load_overwrite_part_tachyon")
      sc.runSql("""create table load_overwrite_part_tachyon (key int, value string)
        partitioned by (keypart int)""")
      sc.runSql("""load data local inpath '%s' overwrite into table load_overwrite_part_tachyon
        partition(keypart = 1)""".format(KV1_TXT_PATH))
      expectSql("select count(*) from load_overwrite_part_tachyon", "500")
      sc.runSql("drop table if exists load_overwrite_part_tachyon")
    }
  }
}
