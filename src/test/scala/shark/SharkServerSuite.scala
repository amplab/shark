package shark

import java.io.{BufferedReader, InputStreamReader}
import java.sql.DriverManager
import java.sql.Statement
import java.sql.Connection

import scala.collection.JavaConversions._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.ShouldMatchers


/**
 * Test for the Shark server.
 */
class SharkServerSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers with TestUtils {

  val WAREHOUSE_PATH = TestUtils.getWarehousePath("server")
  val METASTORE_PATH = TestUtils.getMetastorePath("server")
  val DRIVER_NAME  = "org.apache.hadoop.hive.jdbc.HiveDriver"
  val TABLE = "test"
  // use a different port, than the hive standard 10000,
  // for tests to avoid issues with the port being taken on some machines
  val PORT = "9011"

  Class.forName(DRIVER_NAME)

  override def beforeAll() { launchServer() }

  override def afterAll() { stopServer() }

  private def launchServer(args: Seq[String] = Seq.empty) {
    // Forking a new process to start the Shark server. The reason to do this is it is
    // hard to clean up Hive resources entirely, so we just start a new process and kill
    // that process for cleanup.
    val defaultArgs = Seq("./bin/shark", "--service", "sharkserver",
      "--verbose",
      "-p",
      PORT,
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
    waitForOutput(inputReader, "Starting Shark server")
  }

  private def stopServer() {
    process.destroy()
    process.waitFor()
  }

  test("test query execution against a shark server") {

    val dataFilePath = TestUtils.dataFilePath + "/kv1.txt"
    val stmt = createStatement()
    stmt.executeQuery("DROP TABLE IF EXISTS test")
    stmt.executeQuery("DROP TABLE IF EXISTS test_cached")
    stmt.executeQuery("CREATE TABLE test(key int, val string)")
    stmt.executeQuery("LOAD DATA LOCAL INPATH '" + dataFilePath+ "' OVERWRITE INTO TABLE test")
    stmt.executeQuery("CREATE TABLE test_cached as select * from test limit 499")

    var rs = stmt.executeQuery("select count(*) from test")
    rs.next()
    rs.getInt(1) should equal (500)

    rs = stmt.executeQuery("select count(*) from test_cached")
    rs.next()
    rs.getInt(1) should equal (499)

    stmt.close()
  }

  def getConnection(): Connection = {
    DriverManager.getConnection("jdbc:hive://localhost:" + PORT + "/default", "", "")
  }

  def createStatement(): Statement = getConnection().createStatement()
}