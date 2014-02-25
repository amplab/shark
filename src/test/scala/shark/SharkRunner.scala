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

import org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME

import shark.api.JavaSharkContext
import shark.memstore2.MemoryMetadataManager


object SharkRunner {

  val WAREHOUSE_PATH = TestUtils.getWarehousePath()
  val METASTORE_PATH = TestUtils.getMetastorePath()
  val MASTER = "local"

  var sc: SharkContext = _

  var javaSc: JavaSharkContext = _

  def init(): SharkContext = synchronized {
  	if (sc == null) {
      sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)

      sc.runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" +
      METASTORE_PATH + ";create=true")
      sc.runSql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)
      sc.runSql("set shark.test.data.path=" + TestUtils.dataFilePath)

      // second db
      sc.sql("create database if not exists seconddb")

      loadTables()
    }
    sc
  }

  def initWithJava(): JavaSharkContext = synchronized {
    if (javaSc == null) {
      javaSc = new JavaSharkContext(init())
    }
    javaSc
  }

  /**
   * Tables accessible by any test. Their properties should remain constant across
   * tests.
   */
  def loadTables() = synchronized {
    require(sc != null, "call init() to instantiate a SharkContext first")

    // Use the default namespace
    sc.runSql("USE " + DEFAULT_DATABASE_NAME)

    // test
    sc.runSql("drop table if exists test")
    sc.runSql("CREATE TABLE test (key INT, val STRING)")
    sc.runSql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test")
    sc.runSql("drop table if exists test_cached")
    sc.runSql("CREATE TABLE test_cached tblproperties('shark.cache'='memory') AS SELECT * FROM test")

    // test_null
    sc.runSql("drop table if exists test_null")
    sc.runSql("CREATE TABLE test_null (key INT, val STRING)")
    sc.runSql("""LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv3.txt'
      INTO TABLE test_null""")
    sc.runSql("drop table if exists test_null_cached")
    sc.runSql("CREATE TABLE test_null_cached tblproperties('shark.cache'='memory') AS SELECT * FROM test_null")

    // clicks
    sc.runSql("drop table if exists clicks")
    sc.runSql("""create table clicks (id int, click int)
      row format delimited fields terminated by '\t'""")
    sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/clicks.txt'
      OVERWRITE INTO TABLE clicks""")
    sc.runSql("drop table if exists clicks_cached")
    sc.runSql("create table clicks_cached tblproperties('shark.cache'='memory') as select * from clicks")

    // users
    sc.runSql("drop table if exists users")
    sc.runSql("""create table users (id int, name string)
      row format delimited fields terminated by '\t'""")
    sc.runSql("""load data local inpath '${hiveconf:shark.test.data.path}/users.txt'
      OVERWRITE INTO TABLE users""")
    sc.runSql("drop table if exists users_cached")
    sc.runSql("create table users_cached tblproperties('shark.cache'='memory') as select * from users")

    // test1
    sc.sql("drop table if exists test1")
    sc.sql("""CREATE TABLE test1 (id INT, test1val ARRAY<INT>)
      row format delimited fields terminated by '\t'""")
    sc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test1.txt' INTO TABLE test1")
    sc.sql("drop table if exists test1_cached")
    sc.sql("CREATE TABLE test1_cached tblproperties('shark.cache'='memory') AS SELECT * FROM test1")
    Unit
  }

  def expectSql(sql: String, expectedResults: Array[String], sort: Boolean = true) {
    val sharkResults: Array[String] = sc.runSql(sql).results.map(_.mkString("\t")).toArray
    val results = if (sort) sharkResults.sortWith(_ < _) else sharkResults
    val expected = if (sort) expectedResults.sortWith(_ < _) else expectedResults
    assert(results.corresponds(expected)(_.equals(_)),
      "In SQL: " + sql + "\n" +
      "Expected: " + expected.mkString("\n") + "; got " + results.mkString("\n"))
  }

  // A shortcut for single row results.
  def expectSql(sql: String, expectedResult: String) {
    expectSql(sql, Array(expectedResult))
  }

}
