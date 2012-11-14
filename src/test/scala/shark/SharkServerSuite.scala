package shark

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch

import scala.actors.Actor
import scala.collection.JavaConversions.asScalaConcurrentMap
import scala.concurrent.ops.spawn

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class SharkServerSuite extends FunSuite with BeforeAndAfterAll  with BeforeAndAfterEach with ShouldMatchers {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("server")

  val DRIVER_NAME  = "org.apache.hadoop.hive.jdbc.HiveDriver"
  val TABLE = "test"

  Class.forName(DRIVER_NAME)
  
  override def beforeAll() {
    
    spawn {
      SharkServer.main(Array[String]())
    }
    while(!SharkServer.ready){}
    createTable
    createCachedTable
  }

  override def afterAll() {
    dropTable
    dropCachedTable
    SharkServer.stop
  }
  
  test("Read from existing table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test where key = 406")
    rs.next ; 
    val count = rs.getInt(1)
    println("Count : " , count)
    count should equal (4)
  } 
  
  test("Read from existing cached table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test_cached where key = 406")
    rs.next ; 
    val count = rs.getInt(1)
    count should equal (4)
  } 
 
  test("SHARK-64, persist cached table metadata across sessions" ) {
    //cached table creation in the initialization step should have attached metadata
    assert(SharkCTAS.getMeta.exists(x => x._1.equalsIgnoreCase("test_cached")))
    dropTable("test_cached")
    assert(!SharkCTAS.getMeta.exists(x => x._1.equalsIgnoreCase("test_cached")))
    createCachedTable
    //check the tables have been recreated
    assert(SharkCTAS.getMeta.exists(x => x._1.equalsIgnoreCase("test_cached")))

  }
  
  test("Concurrent reads with state") {
    Class.forName(DRIVER_NAME)
    val n = 3
    val latch = new CountDownLatch(n)
    var results:collection.mutable.ConcurrentMap[Int, Int] = new ConcurrentHashMap[Int,Int]
    class ManagedReadActor(threshold:Int) extends Actor {
          def exec = {
            val con = getConnection
            val setThresholdStmt = con.createStatement()
            setThresholdStmt.executeQuery("set threshold = " + threshold)
            val stmt = con.createStatement()
            val rs = stmt.executeQuery("select  count(*) from test where key = " +
              "${hiveconf:threshold}")
            
            rs.next
            val value = rs.getInt(1)
            results.putIfAbsent(threshold, value)
          }
          
          def act = {
            try {
              exec
            }finally {
              latch.countDown
            }
          }
    }

    List(238, 406, 401).foreach(index => new ManagedReadActor(index).start)
    latch.await
    results should be (Map[Int, Int](238 ->2, 406 -> 4, 401 -> 5))
  }
  
  def getConnection:Connection  = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "")
  
  def createStatement:Statement = {
    getConnection.createStatement()
  }
  
  def createTable() = {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test")
    stmt.executeQuery("CREATE TABLE test(key int, val string)")
    stmt.executeQuery("LOAD DATA LOCAL INPATH '" + dataFilePath+ "' OVERWRITE INTO TABLE test")
  }
  
  def createCachedTable() = {
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test_cached")
    createStatement.executeQuery("CREATE TABLE test_cached as select * from test")
  }
  
  def dropTable(implicit table:String = TABLE) = {
    val stmt = createStatement
    val sql = "DROP TABLE " + table 
    val rs = stmt.executeQuery(sql)
  }
  
  def dropCachedTable = dropTable(TABLE + "_cached")
}