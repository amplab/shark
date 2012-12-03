package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch

import scala.actors.Actor
import scala.collection.JavaConversions._
import scala.concurrent.ops.spawn

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers


class SharkServerSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers
  with CliTestToolkit {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("server")
  val METASTORE_PATH = CliTestToolkit.getMetastorePath("server")

  val TABLE = "test"
  val TABLE_CACHED = "test_cached"
  val CACHED_TBL_DESC =
    (TABLE_CACHED, "CREATE TABLE " + TABLE_CACHED + " as select * from " + TABLE)

  val DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver"
  Class.forName(DRIVER_NAME)

  override def beforeAll() {
    launchServer()
    createTable()
    createCachedTable()
  }

  override def afterAll() {
    stopServer()
  }

  def launchServer(args: Seq[String] = Seq.empty) {
    val defaultArgs = Seq("./bin/shark", "--service", "sharkserver",
      "-hiveconf",
      "javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true",
      "-hiveconf",
      "hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)
    val pb = new ProcessBuilder(defaultArgs ++ args)
    process = pb.start()
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "Starting Shark server")
  }

  def stopServer() {
    process.destroy()
    process.waitFor()
  }

  test("Read from existing table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from " + TABLE + " where key = 406")
    stmt.close
    rs.next
    val count = rs.getInt(1)
    count should equal (4)
  }

  test("Read from existing cached table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from " + TABLE_CACHED + " where key = 406")
    stmt.close
    rs.next
    val count = rs.getInt(1)
    count should equal (4)
  }

  test("Shark-64, keep cache table metadata across sessions") {
    def executeCount(): Int = {
      val stmt = createStatement
      val rs = stmt.executeQuery("select count(*) from " + TABLE_CACHED + " where key = 406")
      stmt.close()
      rs.next
      rs.getInt(1)
    }

    stopServer()
    launchServer()
    executeCount() should equal (0)

    // Stop and restart server, with explicit instruction to load RDDs
    stopServer()
    launchServer(Seq("-loadRdds"))
    executeCount() should equal (4)
  }

  test("Concurrent reads with state") {
    Class.forName(DRIVER_NAME)
    val n = 3
    val latch = new CountDownLatch(n)
    var results: collection.mutable.ConcurrentMap[Int, Int] = new ConcurrentHashMap[Int, Int]

    class ManagedReadActor(threshold:Int) extends Actor {
      def executeCount() {
        val con = getConnection
        val setThresholdStmt = con.createStatement()
        setThresholdStmt.executeQuery("set threshold = " + threshold)
        val stmt = con.createStatement()
        val rs = stmt.executeQuery(
          "select count(*) from " + TABLE + " where key = " + "${hiveconf:threshold}")
        stmt.close
        rs.next
        val value = rs.getInt(1)
        results.putIfAbsent(threshold, value)
      }

      def act: Unit = {
        try {
          executeCount()
        } finally {
          latch.countDown
        }
      }
    }

    List(238, 406, 401).foreach(index => new ManagedReadActor(index).start)
    latch.await
    results should be (Map[Int, Int](238 ->2, 406 -> 4, 401 -> 5))
  }

  def getConnection(): Connection =
    DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "")

  def createStatement(): Statement = getConnection.createStatement()

  def createTable() {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    var stmt = createStatement()
    println("cmd: " + "CREATE TABLE " + TABLE + " (key int, val string)")
    stmt.executeQuery("CREATE TABLE " + TABLE + " (key int, val string)")
    stmt.close()
    stmt = createStatement()
    stmt.executeQuery("LOAD DATA LOCAL INPATH '" + dataFilePath + "' OVERWRITE INTO TABLE " + TABLE)
    stmt.close()
  }

  def createCachedTable() {
    println("executing " + CACHED_TBL_DESC._2)
    var stmt = createStatement()
    stmt.executeQuery(CACHED_TBL_DESC._2)
    stmt.close()
  }
}
