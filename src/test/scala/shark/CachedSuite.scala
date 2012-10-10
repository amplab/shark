package shark

class CachedSuite extends SharkShellCliSuite {

  test("Cached table with simple types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists test;");
    executeQuery("create table test(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table test;")
    executeQuery("create table test_cached as select * from test;")
    val out = executeQuery("select * from test_cached where key = 407;")
    assert(out.contains("val_407"))
  }

  test("Cached Table with complex types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/create_nested_type.txt"
    executeQuery("drop table if exists table1;");
    executeQuery("drop table if exists table1_cached;");
    executeQuery("CREATE TABLE table1 (a STRING, b ARRAY<STRING>, c ARRAY<MAP<STRING,STRING>>, d MAP<STRING,ARRAY<STRING>>);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table table1;")
    executeQuery("""create table table1_cached TBLPROPERTIES ("shark.cache" = "true") as select * from table1;""")
    val out = executeQuery("select * from table1_cached where a = 'a0';")
    assert(out.contains("""{"c001":"C001","c002":"C002"}"""))
  }

}