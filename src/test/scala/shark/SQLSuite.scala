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


class SQLSuite extends FunSuite with BeforeAndAfterAll {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("sql")
  val METASTORE_PATH = CliTestToolkit.getMetastorePath("sql")

  val MASTER = "local"

  var sc: SharkContext = _

  override def beforeAll() {
    sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER);
    sc.sql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" +
      METASTORE_PATH + ";create=true")
    sc.sql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)
    sc.sql("CREATE TABLE src(key INT, value STRING)")
    sc.sql("LOAD DATA LOCAL INPATH '${env:HIVE_HOME}/examples/files/kv1.txt' INTO TABLE src")
    sc.sql("CREATE TABLE src_cached AS SELECT * FROM SRC")
  }

  override def afterAll() {
    sc.stop()
  }

  test("count") {
    expect("select count(*) from src", Array("500"))
    expect("select count(*) from src_cached", Array("500"))
  }

  test("filter") {
    expect("select * from src where key=100 or key=497",
      Array("100\tval_100", "100\tval_100", "497\tval_497"))
    expect("select * from src_cached where key=100 or key=497",
      Array("100\tval_100", "100\tval_100", "497\tval_497"))
  }

  def expect(sql: String, expectedResults: Array[String]) {
    val results = sc.sql(sql).sortWith(_ < _)
    val expected = expectedResults.sortWith(_ < _)
    assert(results.corresponds(expected)(_.equals(_)), "Expected: " + expected.mkString + "; got " + results.mkString)
  }

}
