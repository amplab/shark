package shark

import com.sun.org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Table
import scala.collection.JavaConversions.asScalaBuffer

/**
 * Singleton representing access to the Shark Meta data that gets applied to cached tables
 * in the Hive Meta Store.
 * All cached tables are tagged with a property CTAS_QUERY_STRING whose value
 * represents the query that led to the creation of the cached table.
 * This is used to reload RDDs upon server restarts.
 */
object SharkCTAS {

  val db = Hive.get(new HiveConf)

  val QUERY_STRING = "CTAS_QUERY_STRING"

  private val LOG = LogFactory.getLog("SharkCTAS")

  def loadAsRdds(cmdRunner: String => Unit): Unit = {
    getMeta.foreach(t => {
      try {
        db.dropTable(t._1)
        cmdRunner(t._2)
      } catch {
        case e: Exception => LOG.debug("Reloading cached table failed " + t._1, e);
      }
    })
  }

  def updateMeta(cachedTableQueries : Iterable[(String,String)]): Unit = {
    cachedTableQueries.foreach(x => {
      val newTbl = new Table(db.getTable(x._1).getTTable())
      newTbl.setProperty(QUERY_STRING, x._2)
      db.alterTable(x._1, newTbl)
    })
  }

  def getMeta():Seq[(String, String)] = {
    db.getAllTables().foldLeft(List[(String,String)]())((curr, tableName) => {
      val tbl = db.getTable(tableName)
      Option(tbl.getProperty(QUERY_STRING)) match {
        case Some(q) => curr.::(tableName, q)
        case None => curr
      }
    })
  }

}
