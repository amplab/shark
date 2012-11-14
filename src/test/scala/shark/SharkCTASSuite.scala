package shark

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec
import org.scalatest.FunSuite
import scala.collection.JavaConversions._
import org.scalatest.matchers.ShouldMatchers._
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.scalatest.BeforeAndAfterAll

class SharkCTASSuite extends FunSuite with BeforeAndAfterAll{

  val db = Hive.get(new HiveConf)

  override def beforeAll() {
    db.createTable("test_cached",  List("foo"), List(), classOf[HiveInputFormat[_,_]], classOf[HiveOutputFormat[_,_]])
    db.createTable("test2",  List("foo"), List(), classOf[HiveInputFormat[_,_]], classOf[HiveOutputFormat[_,_]])
  }
  
  override def afterAll() {
    db.dropTable("test_cached")
    db.dropTable("test2")
  }
  
  test("Initialize") {
   
    SharkCTAS.updateMeta(List(("test_cached","create table test_cached as select * from test"),
        ("test2","create table test2_cached as select * from test")))
    SharkCTAS.getMeta should have size (2)
  }
  test("Load Cached Tables") {
     val cachedTables = List(("test_cached","create table test_cached as select * from test"),
        ("test2","create table test2_cached as select * from test"))
     SharkCTAS.updateMeta(cachedTables)
     SharkCTAS.loadAsRdds(x => assert(cachedTables.exists(y => x.equals(y._2))))
  }
}