package shark

import java.io.{BufferedReader, InputStreamReader}
import java.sql.DriverManager
import java.sql.Statement
import java.sql.Connection

import scala.collection.JavaConversions._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent._
import ExecutionContext.Implicits.global

import org.apache.spark.sql.catalyst.util.getTempFilePath

/**
 * Test for the SharkServer2 using JDBC.
 */
class SharkServer2Suite extends FunSuite with BeforeAndAfterAll with TestUtils with Logging {

  val WAREHOUSE_PATH = getTempFilePath("sharkWarehouse")
  val METASTORE_PATH = getTempFilePath("sharkMetastore")

  val DRIVER_NAME  = "org.apache.hive.jdbc.HiveDriver"
  val TABLE = "test"
  // use a different port, than the hive standard 10000,
  // for tests to avoid issues with the port being taken on some machines
  val PORT = "10000"

  // If verbose is true, the testing program will print all outputs coming from the shark server.
  val VERBOSE = Option(System.getenv("SHARK_TEST_VERBOSE")).getOrElse("false").toBoolean

  Class.forName(DRIVER_NAME)

  override def beforeAll() { launchServer() }

  override def afterAll() { stopServer() }

  private def launchServer(args: Seq[String] = Seq.empty) {
    // Forking a new process to start the Shark server. The reason to do this is it is
    // hard to clean up Hive resources entirely, so we just start a new process and kill
    // that process for cleanup.
    val defaultArgs = Seq("./bin/shark", "--service", "sharkserver2",
      "--hiveconf",
      "hive.root.logger=INFO,console",
      "--hiveconf",
      "\"javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true\"",
      "--hiveconf",
      "\"hive.metastore.warehouse.dir=" + WAREHOUSE_PATH + "\"")
    val pb = new ProcessBuilder(defaultArgs ++ args)
    process = pb.start()
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "ThriftBinaryCLIService listening on")

    // Spawn a thread to read the output from the forked process.
    // Note that this is necessary since in some configurations, log4j could be blocked
    // if its output to stderr are not read, and eventually blocking the entire test suite.
    future {
      while (true) {
        val stdout = readFrom(inputReader)
        val stderr = readFrom(errorReader)
        if (VERBOSE && stdout.length > 0) {
          println(stdout)
        }
        if (VERBOSE && stderr.length > 0) {
          println(stderr)
        }
        Thread.sleep(50)
      }
    }
  }

  private def stopServer() {
    process.destroy()
    process.waitFor()
  }

  test("test query execution against a shark server") {
    Thread.sleep(5 * 1000)
    val dataFilePath = TestUtils.dataFilePath + "/kv1.txt"
    val stmt = createStatement()
    stmt.execute("DROP TABLE IF EXISTS test")
    stmt.execute("DROP TABLE IF EXISTS test_cached")
    stmt.execute("CREATE TABLE test(key int, val string)")
    stmt.execute(s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test")
    stmt.execute("CREATE TABLE test_cached as select * from test limit 499")

    var rs = stmt.executeQuery("select count(*) from test")
    rs.next()
    assert(rs.getInt(1) === 500)

    rs = stmt.executeQuery("select count(*) from test_cached")
    rs.next()
    assert(rs.getInt(1) === 499)

    stmt.close()
  }

  def getConnection(): Connection = {
    val connectURI = s"jdbc:hive2://localhost:$PORT/"
    DriverManager.getConnection(connectURI, "", "")
  }

  def createStatement(): Statement = getConnection().createStatement()
}
