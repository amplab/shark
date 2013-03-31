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

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CachedSuite extends FunSuite with BeforeAndAfterAll with CliTestToolkit {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("cli")
  val METASTORE_PATH = CliTestToolkit.getMetastorePath("cli")

  override def beforeAll() {
    val pb = new ProcessBuilder(
      "./bin/shark",
      "-hiveconf",
      "javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true",
      "-hiveconf",
      "hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

    process = pb.start()
    outputWriter = new PrintWriter(process.getOutputStream, true)
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "shark>")
  }

  override def afterAll() {
    process.destroy()
    process.waitFor()
  }

  test("Cached table with simple types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("create table shark_test1(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test1;")
    executeQuery("""create table shark_test1_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test1;""")
    val out = executeQuery("select * from shark_test1_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("shark_test1_cached"))
  }

  test("Cached Table with complex types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/create_nested_type.txt"
    executeQuery("CREATE TABLE shark_test2 (a STRING, b ARRAY<STRING>, c ARRAY<MAP<STRING,STRING>>, d MAP<STRING,ARRAY<STRING>>);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test2;")
    executeQuery("""create table shark_test2_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test2;""")
    val out = executeQuery("select * from shark_test2_cached where a = 'a0';")
    assert(out.contains("""{"c001":"C001","c002":"C002"}"""))
    assert(isCachedTable("shark_test2_cached"))
  }

  test("Tables are not cached by default") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("set shark.cache.flag.checkTableName=false; " +
      "create table shark_test3(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test3;")
    executeQuery("""create table shark_test3_cached as select * from shark_test3;""")
    val out = executeQuery("select * from shark_test3_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(!isCachedTable("shark_test3_cached"))
  }

  test("_cached table are cachd when checkTableName flag is set") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("set shark.cache.flag.checkTableName=true; " +
      "create table shark_test4(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test4;")
    executeQuery("""create table shark_test4_cached as select * from shark_test4;""")
    val out = executeQuery("select * from shark_test4_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("shark_test4_cached"))
  }

  test("cached table name are case-insensitive") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("create table shark_test5(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test5;")
    executeQuery("""create table sharkTest5Cached TBLPROPERTIES ("shark.cache" = "true") """ +
      """ as select * from shark_test5;""")
    val out = executeQuery("select * from sharktest5Cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("sharkTest5Cached"))
  }

  test("Update single cached table using INSERT") {
    val kv1FilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val kv2FilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv2.txt"
    executeQuery("create table shark_test6_kv1(key int, val string);")
    executeQuery("create table shark_test6_kv2(key int, val string);")
    executeQuery("load data local inpath '" + kv1FilePath+ "' overwrite into table shark_test6_kv1;")
    executeQuery("load data local inpath '" + kv2FilePath+ "' overwrite into table shark_test6_kv2;")
    executeQuery("""create table shark_test6_kv1kv2_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test6_kv1;""")
    executeQuery("insert into table shark_test6_kv1kv2_cached select * from shark_test6_kv2;")
    val cachedKeySum = executeQuery("select sum(key) from shark_test6_kv1kv2_cached;")
    // sum(kv1.key) + sum(kv2.key)
    assert(cachedKeySum.contains("257965"))
    assert(isCachedTable("shark_test6_kv1kv2_cached"))
  }

  test("Overwrite cached table using INSERT") {
    val kv1FilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val kv2FilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv2.txt"
    executeQuery("create table shark_test9_kv1(key int, val string);")
    executeQuery("create table shark_test9_kv2(key int, val string);")
    executeQuery("load data local inpath '" + kv1FilePath+ "' overwrite into table shark_test9_kv1;")
    executeQuery("load data local inpath '" + kv2FilePath+ "' overwrite into table shark_test9_kv2;")
    executeQuery("""create table shark_test9_kv1kv2_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test9_kv1;""")
    executeQuery("insert overwrite table shark_test9_kv1kv2_cached select * from shark_test9_kv2;")
    val cachedKeySum = executeQuery("select sum(key) from shark_test9_kv1kv2_cached;")
    // sum(kv2.key)
    assert(cachedKeySum.contains("127874"))
    assert(isCachedTable("shark_test9_kv1kv2_cached"))
  }

  test("Returns error when attempting to update cached table(s) using command with multiple INSERTs") {
    val kv1FilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val kv2FilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv2.txt"
    executeQuery("create table shark_test7_kv1(key int, val string);")
    executeQuery("create table shark_test7_kv2(key int, val string);")
    executeQuery("load data local inpath '" + kv1FilePath+ "' overwrite into table shark_test7_kv1;")
    executeQuery("load data local inpath '" + kv2FilePath+ "' overwrite into table shark_test7_kv2;")
    executeQuery("""create table shark_test7_kv1_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test7_kv2;""")
    val multipleInsertQuery = "from shark_test7_kv2 " +
      "insert into table shark_test7_kv1 select * " +
      "insert into table shark_test7_kv1_cached select *;"
    executeQuery(multipleInsertQuery, "Shark does not support updating cached table(s) with multiple INSERTs")
  }

  def isCachedTable(tableName: String) : Boolean = {
    val dir = new File(WAREHOUSE_PATH + "/" + tableName.toLowerCase)
    dir.isDirectory && dir.listFiles.isEmpty
  }
}