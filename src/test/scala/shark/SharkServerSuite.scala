package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfterAll, FunSuite, BeforeAndAfterEach, FlatSpec}
import java.sql.DriverManager
import org.scalatest.Assertions
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.sql.Connection
import scala.actors.Actor
import java.util.concurrent.ConcurrentHashMap
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import scala.collection.JavaConversions._
import scala.concurrent.ops._
import spark.SparkEnv

class SharkServerSuite extends FunSuite with BeforeAndAfterAll  with BeforeAndAfterEach with ShouldMatchers {

  val WAREHOUSE_PATH = TestUtils.getWarehousePath("server")
  val METASTORE_PATH = TestUtils.getMetastorePath("server")
  val DRIVER_NAME  = "org.apache.hadoop.hive.jdbc.HiveDriver"
  val TABLE = "test"

  Class.forName(DRIVER_NAME)
  
  override def beforeAll() {
    
    spawn {
      SharkServer.main(Array("-hiveconf", 
          "javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" +
          METASTORE_PATH + ";create=true",
          "-hiveconf",
          "hive.metastore.warehouse.dir=" + WAREHOUSE_PATH))
    }
    while(!SharkServer.ready){}
    createTable
    createCachedTable
    val stmt = createStatement
    val baseFilePath = System.getenv("HIVE_DEV_HOME")
    stmt.executeQuery("create table test_bigint (key bigint, val string)")
    val kvPath = baseFilePath + "/data/files/kv1.txt"
    stmt.executeQuery("load data local inpath '" + kvPath + "' OVERWRITE INTO TABLE test_bigint")
  }

  override def afterAll() {
    dropTable
    dropTable("foo_cached")
    dropTable("test_bigint")
    dropTable("test_bigint_cached")
    dropTable("a")
    dropTable("a_cached")
    dropCachedTable
    SharkServer.stop
  
  }

  test("Count Distinct ") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(distinct key) from test")
    rs.next;
    stmt.close
    val count = rs.getInt(1)
    count should equal(309)
  }
  
  test("Count bigint ") {
    val stmt = createStatement
    stmt.executeQuery("""create table test_bigint_cached as select * from test_bigint""")
    val rs = stmt.executeQuery("select val, count(*) from test_bigint_cached where key=484 group by val")
    rs.next;
    stmt.close
    val count = rs.getInt(2)
    count should equal(1)
  }

  test("Read from existing table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select   count(*) from test")
    rs.next ; 
    stmt.close
    val count = rs.getInt(1)
    count should equal (500)
  } 
  
  test("column pruning, count distinct on join with cached tbls") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(distinct x.val) from test_cached x join test_cached y on (x.key = y.key) ")
    //check if this worked
    rs.next ; 
    stmt.close
    val count = rs.getInt(1)
    count should equal (309)
  }

  test("column pruning, join condition1") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test_cached left outer join test on (test_cached.key = test.key) ")
    //check if this worked
    rs.next ; 
    stmt.close
    val count = rs.getInt(1)
    count should equal (1028)
  }
  
  test("column pruning filters") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test_cached where key > -1 ")
    //check if this worked
    rs.next ; 
    stmt.close
    val count = rs.getInt(1)
    count should equal (500)
  }
  
  test("column pruning group by") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select key, count(*) from test_cached group by key ")
    //check if this worked
    rs.next ; 
    stmt.close
    val count = rs.getInt(1)
    count should equal (487)
  }

  test("column pruning group by with single filter") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select key, count(*) from test_cached where val='val_484' group by key ")
    //check if this worked
    rs.next ; 
    stmt.close
    val count = rs.getInt(1)
    count should equal (484)
  }

  test("column pruning aggregate function") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select val, sum(key) from test_cached group by val order by val desc")
    //check if this worked
    rs.next;
    stmt.close
    val count = rs.getInt(2)
    count should equal(196)
  }

  test("insert into cached table") {
    val stmt = createStatement
    stmt.executeQuery("insert into table test_cached select * from test where key > -1")
    val rs = stmt.executeQuery("select count(*) from test_cached")
    //check if this worked
    rs.next;
    val count = rs.getInt(1)
    stmt.close
    count should equal(1000)
  }

  test("drop table") {
    val stmt = createStatement
    stmt.executeQuery("""create table foo_cached as select * from test""")
    stmt.executeQuery("insert into table foo_cached select * from test")
    //at this point we should have 1000 entries
    val rs = stmt.executeQuery("select count(*) from foo_cached")
    assert(SharkEnv.memoryMetadataManager.contains("foo_cached") == true)
    //check if this worked
    rs.next;
    val count = rs.getInt(1)
    stmt.close
    count should equal(1000)
    val stmt2 = createStatement
    stmt2.executeQuery("drop table foo_cached")
    //at this point the table should have been removed from cache as well as from blockmanager
    assert(SharkEnv.memoryMetadataManager.contains("foo_cached") == false)
  }
  
  test("drop nonexistent table") {
    val stmt = createStatement
    try {
    	stmt.executeQuery("drop table bar_cached")
    } catch {
    	case e: Throwable => fail("dropping non existent tables should be ok")
    }
    
  }
  
  test("drop noncached table") {
    val stmt = createStatement
    try {
    	stmt.executeQuery("drop table test")
    } catch {
    	case e: Throwable => fail("dropping non cached tables should be ok")
    }
    
  }
  
  def getConnection:Connection  = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "")
  
  def createStatement:Statement = {
    getConnection.createStatement()
  }
  
  def createTable(implicit table:String = TABLE) = {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test")
    stmt.executeQuery("CREATE TABLE test(key int, val string)")
    stmt.executeQuery("LOAD DATA LOCAL INPATH '" + dataFilePath+ "' OVERWRITE INTO TABLE test")
  }
  
  def createCachedTable() = {
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test_cached")
    stmt.executeQuery("CREATE TABLE test_cached as select * from test")
  }
  
  def dropTable(implicit table:String = TABLE) = {
    val stmt = createStatement
    val sql = "DROP TABLE " + table 
    val rs = stmt.executeQuery(sql)
  }
  
  def dropCachedTable = dropTable(TABLE + "_cached")
}