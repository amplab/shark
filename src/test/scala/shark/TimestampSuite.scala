package shark

import java.io.{FileOutputStream, PrintWriter, BufferedOutputStream, File}
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.Assert._

/**
 * Test various queries related to timestamps. Specifically, test for a bug that is occurs when
 * counting timestamp values, ordering by the same timestamp column.
 */
class TimestampSuite extends FunSuite with SharkHiveTestUtil with BeforeAndAfterAll {

  private[this] var sc: SharkContext = _

  override def beforeAll() {
    sc = SharkEnv.initWithSharkContext("TimestampSuite")
    setHiveTestDir()
  }

  override def afterAll() {
    SharkEnv.stop()
    System.clearProperty("spark.driver.port")
  }

  def createTestTableData(tableName: String)(writeFile: (PrintWriter) => Unit): String = {
    val testDir = getTestDir
    new File(testDir).mkdirs()
    val testDataFile = new File(testDir, tableName + ".tsv")
    val out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(testDataFile)))
    try {
      writeFile(out)
      testDataFile.getAbsolutePath
    } finally {
      out.close()
    }
  }

  // Temporarily disabled: breaks SQLSuite
  ignore("CountGroupByOrderByTimestamp") {

    val tableName = "ts_test"
    val tsStr = "2013-03-18 00:41:15"
    val testDataPath = createTestTableData(tableName) { out =>
      out.println(tsStr)
    }

    assertEquals("", sc.sql("DROP TABLE IF EXISTS %s".format(tableName)).mkString("\n"))
    assertEquals("", sc.sql("CREATE TABLE %s (t TIMESTAMP)".format(tableName)).mkString("\n"))
    assertEquals("", sc.sql("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s".format(
      testDataPath, tableName)).mkString("\n"))
    assertEquals(tsStr, sc.sql("SELECT * FROM %s".format(tableName)).mkString("\n"))
    assertEquals(tsStr + "\t1",
      sc.sql("SELECT t, COUNT(1) FROM %s GROUP BY t ORDER BY t".format(tableName)).mkString("\n"))
  }

  private[this] def createTable(
      tableName: String,
      ddl: String,
      additionalDDL: String*)() {
    assertEquals("", sc.sql("CREATE TABLE " + tableName + " " + ddl).mkString("\n"))
    additionalDDL.foreach { operation =>
      assertEquals("", sc.sql(operation).mkString("\n"))
    }
  }

  private[this] def dropTable(tableName: String) {
    assertEquals("", sc.sql("DROP TABLE IF EXISTS " + tableName).mkString("\n"))
  }

  private[this] def verifyRepetitionNumber(tableName: String, repetitions: Int) {
    assertEquals("",
      sc.sql("select * from (select c_date, count(*) as n from " + tableName +
             " group by c_date) subquery where n != " + repetitions).mkString("\n"))
  }

  // Temporarily disabled: breaks SQLSuite
  ignore("CTASTimestampLoad") {
    val tableName = "ts_repeated"

    val repetitionNumber = 5
    val baseYear = 2020
    val numYears = 50
    val dataPath = createTestTableData(tableName) { out =>
      for (year <- baseYear until baseYear + numYears;
           month <- 1 to 12;
           day <- 1 to 28;
           i <- 1 to repetitionNumber) {
        out.println("%04d-%02d-%02d 00:00:00".format(year, month, day))
      }
    }
    dropTable(tableName)
    createTable(tableName,
      "(c_date TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
      "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s".format(dataPath, tableName))

    val ctasTableName = tableName + "_ctas"
    dropTable(ctasTableName)
    createTable(ctasTableName, "stored as rcfile as select * from " + tableName)

    verifyRepetitionNumber(tableName, repetitionNumber)
    verifyRepetitionNumber(ctasTableName, repetitionNumber)
  }


}
