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
    val clicksPath = baseFilePath + "/data/files/clicks.txt"
    stmt.executeQuery("create table clicks(id int, click int) row format delimited fields terminated by '\t'")
    stmt.executeQuery("load data local inpath '" + clicksPath + "' OVERWRITE INTO TABLE clicks")
    stmt.executeQuery("create table users(id int, name string) row format delimited fields terminated by '\t'")
    val usersPath = baseFilePath + "/data/files/users.txt"
    stmt.executeQuery("load data local inpath '" + usersPath + "' OVERWRITE INTO TABLE users")
    stmt.executeQuery("create table clicks_cached as select * from clicks")
    stmt.executeQuery("create table users_cached as select * from users")
    stmt.executeQuery("create table test_bigint (key bigint, val string)")
    val kvPath = baseFilePath + "/data/files/kv1.txt"
    stmt.executeQuery("load data local inpath '" + kvPath + "' OVERWRITE INTO TABLE test_bigint")
  }

  override def afterAll() {
    dropTable
    dropTable("foo_cached")
    dropTable("clicks")
    dropTable("users")
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
  
  test("mapside join") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select /*+ MapJoin(clicks) */ users.name, count(clicks.click) from clicks join users on (clicks.id = users.id) group by users.name having users.name='A'")
    //check if this worked
    rs.next;
    stmt.close
    val value = rs.getString(1)
    value should equal("A")
    val count = rs.getInt(2)
    count should equal(3)
  }

  test("mapside join2") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select /*+ MapJoin(clicks) */ count(*) from clicks join users on (clicks.id = users.id) ")
    //check if this worked
    rs.next;
    stmt.close
    val value = rs.getInt(1)
    value should equal(5)
  }
  
  test("drop partition") {
    val stmt = createStatement
    stmt.executeQuery("""create table foo_cached(key int, val string) partitioned by (dt string)""")
    stmt.executeQuery("insert overwrite table foo_cached partition(dt='100') select * from test")
    //at this point we should have 500 entries
    val rs = stmt.executeQuery("select count(*) from foo_cached")
    //check if this worked
    rs.next;
    val count = rs.getInt(1)
    stmt.close
    count should equal(500)
    val stmt2 = createStatement
    stmt2.executeQuery("alter table foo_cached drop partition(dt='100')")
    val rs2 = stmt2.executeQuery("select count(*) from foo_cached")
    rs2.next;
    val count2 = rs2.getInt(1)
    stmt2.close
    count2 should equal(0)
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