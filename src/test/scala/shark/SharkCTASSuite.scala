package shark

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.ql.metadata.Hive

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._


// class SharkCTASSuite extends FunSuite with BeforeAndAfterAll {

//   val db = Hive.get(new HiveConf)

//   override def beforeAll() {
//     db.createTable(
//       "foo2_cached",
//     	List("foo"),
//     	List(),
//     	classOf[HiveInputFormat[_,_]],
//     	classOf[HiveOutputFormat[_,_]])

//     db.createTable(
//       "foo2",
//       List("foo"),
//       List(),
//       classOf[HiveInputFormat[_,_]],
//       classOf[HiveOutputFormat[_,_]])
//   }

//   override def afterAll() {
//     db.dropTable("foo2_cached")
//     db.dropTable("foo2")
//     Hive.closeCurrent()
//   }

//   test("Initialize") {
//     val meta = List(("foo2_cached","create table foo2_cached as select * from test"),
//         ("foo2","create table foo2_cached as select * from test"))

//     SharkCTAS.updateMeta(meta)
//     SharkCTAS.getMeta.containsAll(meta)
//   }

//   test("Load Cached Tables") {
//     val cachedTables = List(
//       ("foo2_cached","create table foo2_cached as select * from test"),
//       ("foo2","create table foo2_cached as select * from test"))
//     SharkCTAS.updateMeta(cachedTables)
//     SharkCTAS.loadAsRdds(x => assert(cachedTables.exists(y => x.equals(y._2))))
//   }
// }