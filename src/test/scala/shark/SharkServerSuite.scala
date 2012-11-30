package shark

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import scala.actors.Actor
import scala.collection.JavaConversions.asScalaConcurrentMap
import scala.concurrent.ops.spawn

class SharkServerSuite extends FunSuite with BeforeAndAfterAll  with BeforeAndAfterEach with ShouldMatchers {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("server")

  val DRIVER_NAME  = "org.apache.hadoop.hive.jdbc.HiveDriver"

  val TABLE = "test"

  val CACHED_TBL_DESC = ("test_cached", "CREATE TABLE test_cached as select * from test")

  Class.forName(DRIVER_NAME)

  override def beforeAll() {
    launchServer()
    createTable
    createCachedTable
  }

  override def afterAll() {

    dropTable
    dropCachedTable
    SharkServer.stop
  }

  def launchServer(args:Array[String] = Array[String]()) = {
    spawn {
      SharkServer.main(args)
    }
    while (!SharkServer.ready) {}
  }

  test("Read from existing table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test where key = 406")
    stmt.close
    rs.next
    val count = rs.getInt(1)
    count should equal (4)
  }

  test("Read from existing cached table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test_cached where key = 406")
    stmt.close
    rs.next
    val count = rs.getInt(1)
    count should equal (4)
  }

  test("Shark-64, keep cache table metadata across sessions") {
    //drop cache, load rdds, and check if it worked
    SharkEnv.cache.drop
    def count: Int = {
      val stmt = createStatement
      val rs = stmt.executeQuery("select count(*) from test_cached where key = 406")
      stmt.close
      rs.next;
      rs.getInt(1)
    }

    SharkServer.stop
    launchServer()
    count should equal (0)
    //stop and restart server, with explicit instruction to load RDDs
    SharkServer.stop
    launchServer(Array("-loadRdds"))
    count should equal (4)
  }

  test("Concurrent reads with state") {
    Class.forName(DRIVER_NAME)
    val n = 3
    val latch = new CountDownLatch(n)
    var results:collection.mutable.ConcurrentMap[Int, Int] = new ConcurrentHashMap[Int,Int]
    class ManagedReadActor(threshold:Int) extends Actor {
      def exec: Unit = {
        val con = getConnection
        val setThresholdStmt = con.createStatement()
        setThresholdStmt.executeQuery("set threshold = " + threshold)
        val stmt = con.createStatement()
        val rs = stmt.executeQuery("select  count(*) from test where key = " +
          "${hiveconf:threshold}")
        stmt.close
        rs.next
        val value = rs.getInt(1)
        results.putIfAbsent(threshold, value)
      }

      def act: Unit = {
        try {
          exec
        } finally {
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

  def createTable(): Unit = {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test")
    stmt.executeQuery("CREATE TABLE test(key int, val string)")
    stmt.executeQuery("LOAD DATA LOCAL INPATH '" + dataFilePath + "' OVERWRITE INTO TABLE test")
    stmt.close
  }

  def createCachedTable(): Unit = {
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test_cached")
    createStatement.executeQuery(CACHED_TBL_DESC._2)
    stmt.close
  }

  def dropTable(implicit table:String = TABLE): Unit = {
    val stmt = createStatement
    val sql = "DROP TABLE " + table
    val rs = stmt.executeQuery(sql)
    stmt.close
  }

  def dropCachedTable = dropTable(TABLE + "_cached")
}
