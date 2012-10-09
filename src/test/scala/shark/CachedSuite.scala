package shark

class CachedSuite extends SharkShellCliSuite {

  test("Simple Cached Table") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists test;");
    executeQuery("drop table if exists test_cached;");
    executeQuery("create table test(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table test;")
    executeQuery("create table test_cached as select * from test; ")
    val out = executeQuery("select * from test_cached where key = 407;")
    assert(out.contains("val_407"))
  }

}