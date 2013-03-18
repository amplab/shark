package shark

import java.io.{FileOutputStream, PrintWriter, BufferedOutputStream, File}
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.Assert._

/**
 * Test various queries related to timestamps. Specifically, test for a bug that is occurs when
 * counting timestamp values, ordering by the same timestamp column.
 */
class TimestampSuite extends FunSuite with SharkHiveTestUtil with BeforeAndAfterAll {

  override def beforeAll() {
    setHiveTestDir()
  }

  test("CountGroupByOrderByTimestamp") {
    SharkEnv.initWithSharkContext("CountGroupByOrderByTimestamp")
    val tableName = "ts_test"
    new File(getTestDir).mkdirs()
    val testDataFile = new File(getTestDir, tableName + ".tsv")
    val out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(testDataFile)))
    val tsStr = "2013-03-18 00:41:15"
    out.println(tsStr)
    out.close()

    try {
      val sc = SharkEnv.sc.asInstanceOf[SharkContext]
      assertEquals("", sc.sql("DROP TABLE IF EXISTS %s".format(tableName)).mkString("\n"))
      assertEquals("", sc.sql("CREATE TABLE %s (t TIMESTAMP)".format(tableName)).mkString("\n"))
      assertEquals("", sc.sql("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s".format(
        testDataFile.getAbsolutePath, tableName)).mkString("\n"))
      assertEquals(tsStr, sc.sql("SELECT * FROM %s".format(tableName)).mkString("\n"))
      assertEquals(tsStr + "\t1",
        sc.sql("SELECT t, COUNT(1) FROM %s GROUP BY t ORDER BY t".format(tableName)).mkString("\n"))
    } finally {
      SharkEnv.stop()
    }
  }

}
