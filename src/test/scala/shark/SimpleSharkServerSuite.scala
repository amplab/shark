package shark

class SimpleSharkServerSuite extends SharkServerSuite {
  
  test("Simple Query against Shark Server") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists test;");
    executeQuery("drop table if exists test_cached;");
    executeQuery("create table test(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table test;", 25000)
    executeQuery("create table test_cached as select * from test;", 25000)
    val out = executeQuery("select * from test_cached where key = 407;")
    assert(out.contains("val_407"))
  }

}