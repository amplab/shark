package shark

import java.io.File

class CachedSuite extends SharkShellCliSuite {

  val wareHousePath = "/user/hive/warehouse"

  test("Cached table with simple types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists shark_test1;");
    executeQuery("drop table if exists shark_test1_cached;");
    executeQuery("create table shark_test1(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test1;")
    executeQuery("""create table shark_test1_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test1;""")
    val out = executeQuery("select * from shark_test1_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("shark_test1_cached"))
  }

  test("Cached Table with complex types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/create_nested_type.txt"
    executeQuery("drop table if exists shark_test2;");
    executeQuery("drop table if exists shark_test2_cached;");
    executeQuery("CREATE TABLE shark_test2 (a STRING, b ARRAY<STRING>, c ARRAY<MAP<STRING,STRING>>, d MAP<STRING,ARRAY<STRING>>);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test2;")
    executeQuery("""create table shark_test2_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test2;""")
    val out = executeQuery("select * from shark_test2_cached where a = 'a0';")
    assert(out.contains("""{"c001":"C001","c002":"C002"}"""))
    assert(isCachedTable("shark_test2_cached"))
  }

  test("Tables are not cached by default") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists shark_test3;");
    executeQuery("drop table if exists shark_test3_cached;");
    executeQuery("create table shark_test3(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test3;")
    executeQuery("""create table shark_test3_cached as select * from shark_test3;""")
    val out = executeQuery("select * from shark_test3_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(!isCachedTable("shark_test3_cached"))
  }

  test("_cached table are cachd when checkTableName flag is set") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("set shark.cache.flag.checkTableName=true; drop table if exists shark_test4;");
    executeQuery("drop table if exists shark_test4_cached;");
    executeQuery("create table shark_test4(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test4;")
    executeQuery("""create table shark_test4_cached as select * from shark_test4;""")
    val out = executeQuery("select * from shark_test4_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("shark_test4_cached"))
  }

  test("cached table name are case-insensitive") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists shark_test5;");
    executeQuery("drop table if exists sharkTest5Cached;");
    executeQuery("create table shark_test5(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test5;")
    executeQuery("""create table sharkTest5Cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test5;""")
    val out = executeQuery("select * from sharktest5Cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("sharkTest5Cached"))
  }



  def isCachedTable(tableName: String) : Boolean = {
    val dir = new File(wareHousePath + "/" + tableName)
    dir.isDirectory && dir.listFiles.isEmpty
  }

}