package shark

import java.io.{PrintStream, ByteArrayOutputStream}
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

class CachedTableRecoverySuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val db = CachedTableRecovery.db
  val ss = SessionState.start(db.getConf())
  val errStream = new ByteArrayOutputStream
  val outStream = new ByteArrayOutputStream

  override def beforeAll() {
    createTableMeta("foo2_cached")
    createTableMeta("foo2")
    ss.err = new PrintStream(errStream)
    ss.out = new PrintStream(outStream)
  }

  override def afterAll() {
    errStream.close()
    outStream.close()
    ss.err = null
    ss.out = null
    db.dropTable("foo2_cached")
    db.dropTable("foo2")
    Hive.closeCurrent()
  }

  override def beforeEach() {
    errStream.reset
    outStream.reset
  }
  test("Initialize") {
    val meta = List(("foo2_cached", "create table foo2_cached as select * from test"),
      ("foo2", "create table foo2 as select * from test"))

    CachedTableRecovery.updateMeta(meta)
    assert(CachedTableRecovery.getMeta.containsAll(meta))
  }

  test("Load Cached Tables") {
    val cachedTables = List(
      ("foo2_cached", "create table foo2_cached as select * from test"),
      ("foo2", "create table foo2 as select * from test"))
    CachedTableRecovery.updateMeta(cachedTables)
    var count = 2
    CachedTableRecovery.loadAsRdds(x => {
      cachedTables.find(y => x.equals(y._2)) match {
        case Some(t) => createTableMeta(t._1); count -= 1
        case None => Unit
      }
    })
    assert(errStream.toString().isEmpty())
    assert(outStream.toString().isEmpty())
    count should equal(0)
  }

  test("Error logging") {
    val cachedTables = List(("foo2", "create table foo2_cached as select * from test"))
    CachedTableRecovery.updateMeta(cachedTables)
    CachedTableRecovery.loadAsRdds(x => {
      cachedTables.find(y => x.equals(y._2)) match {
        case Some(t) => createTableMeta(t._1); throw new RuntimeException("Foo")
        case None => Unit
      }
    })
    assert(errStream.toString().contains("Foo"))
  }

  def createTableMeta(name: String): Unit = {
     db.createTable(
      name,
      List("foo"),
      List(),
      classOf[HiveInputFormat[_, _]],
      classOf[HiveOutputFormat[_, _]])
  }
}