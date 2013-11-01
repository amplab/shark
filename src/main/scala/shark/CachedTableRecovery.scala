/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.asScalaSet

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Table


/**
 * Singleton representing access to the Shark Meta data that gets applied to cached tables
 * in the Hive Meta Store.
 * All cached tables are tagged with a property CTAS_QUERY_STRING whose value
 * represents the query that led to the creation of the cached table.
 * This is used to reload RDDs upon server restarts.
 */
object CachedTableRecovery extends LogHelper {

  val db = Hive.get(new HiveConf)

  val QUERY_STRING = "CTAS_QUERY_STRING"

  /**
   * Load the cached tables into memory.
   * @param cmdRunner , the runner that is responsible
   *        for taking a cached table query and
   *        a) create the table metadata in Hive Meta Store
   *        b) load the table as an RDD in memory
   *        @see SharkServer for an example usage.
   */
  def loadAsRdds(cmdRunner: String => Unit) {
    getMeta.foreach { t =>
      try {
        db.dropTable(t._1)
        val tblProps : Map[String,String] = t._3
        
        tblProps.foreach { tblProp =>
          cmdRunner("set "+ tblProp._1 + "=" + tblProp._2 )
        }

        cmdRunner(t._2)
      } catch {
        case e: Exception => logError("Failed to reload cache table " + t._1, e)
      }
    }
  }

  /**
   * Updates the Hive metastore, with cached table metadata.
   * The cached table metadata is stored in the Hive metastore
   * of each cached table, as key value pairs.
   * The key for the first pair being CTAS_QUERY_STRING 
   *   and the value being the cached table query itself.
   * All properties from the hiveconf are read 
   *    and added as the remaining key value pairs.
   *
   * @param cachedTableQueries , a collection of tuples of the form
   *        (cached table name, cached table query, hive conf).
   */
  def updateMeta(cachedTableQueries : Iterable[(String, String, HiveConf)]): Unit = {
    cachedTableQueries.foreach { x =>
      val newTbl = new Table(db.getTable(x._1).getTTable())
      newTbl.setProperty(QUERY_STRING, x._2)
      val hconf = x._3.asInstanceOf[HiveConf]
      hconf.getAllProperties().keySet().foreach { k =>
         val key = k.toString()
         newTbl.setProperty(key, hconf.get(key))
      }
      db.alterTable(x._1, newTbl)
    }
  }

  /**
   * Returns all the Cached table metadata present in the Hive Meta store.
   *
   * @return sequence of tuples, each tuple representing the cached table name,
   *         cached table query and the table properties.
   */
  def getMeta(): Seq[(String, String, Map[String,String])] = {
    db.getAllTables().reverse.foldLeft(List[(String,String,Map[String,String])]())((curr, tableName) => {
      val tbl = db.getTable(tableName)
      Option(tbl.getProperty(QUERY_STRING)) match {
        case Some(q) => curr.::(tableName, q, tbl.getParameters().toMap)
        case None => curr
      }
    })
  }
}