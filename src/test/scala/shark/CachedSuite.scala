package shark

import java.io.File
import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CachedSuite extends FunSuite with BeforeAndAfterAll with CliTestToolkit {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("cli")

  override def beforeAll() {
    val pb = new ProcessBuilder("./bin/shark")
    process = pb.start()
    outputWriter = new PrintWriter(process.getOutputStream, true)
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "shark>")
    outputWriter.write("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH + ";\n")
    outputWriter.flush()
    waitForOutput(inputReader, "shark>")
  }

  override def afterAll() {
    process.destroy()
    process.waitFor()
  }

  test("Cached table with simple types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    dropTable("shark_test1")
    dropTable("shark_test1_cached")
    executeQuery("create table shark_test1(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test1;")
    executeQuery("""create table shark_test1_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test1;""")
    val out = executeQuery("select * from shark_test1_cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("shark_test1_cached"))
  }

  test("Cached Table with complex types") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/create_nested_type.txt"
    dropTable("shark_test2")
    dropTable("shark_test2_cached")
    executeQuery("CREATE TABLE shark_test2 (a STRING, b ARRAY<STRING>, c ARRAY<MAP<STRING,STRING>>, d MAP<STRING,ARRAY<STRING>>);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test2;")
    executeQuery("""create table shark_test2_cached TBLPROPERTIES ("shark.cache" = "true") as select * from shark_test2;""")
    val out = executeQuery("select * from shark_test2_cached where a = 'a0';")
    assert(out.contains("""{"c001":"C001","c002":"C002"}"""))
    assert(isCachedTable("shark_test2_cached"))
  }

  test("Tables are not cached by default") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    dropTable("shark_test3")
    dropTable("shark_test3_cached");
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
    dropTable("shark_test4")
    dropTable("shark_test4_cached")
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
    dropTable("shark_test5")
    dropTable("sharkTest5Cached")
    executeQuery("create table shark_test5(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table shark_test5;")
    executeQuery("""create table sharkTest5Cached TBLPROPERTIES ("shark.cache" = "true") """ +
      """ as select * from shark_test5;""")
    val out = executeQuery("select * from sharktest5Cached where key = 407;")
    assert(out.contains("val_407"))
    assert(isCachedTable("sharkTest5Cached"))
  }

  def isCachedTable(tableName: String) : Boolean = {
    val dir = new File(WAREHOUSE_PATH + "/" + tableName.toLowerCase)
    dir.isDirectory && dir.listFiles.isEmpty
  }
}