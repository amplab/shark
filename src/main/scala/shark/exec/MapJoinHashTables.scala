package shark.exec

import java.util.{ List => JavaList }

import org.apache.hadoop.hive.ql.exec.{ ExprNodeEvaluator, JoinUtil }
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import scala.collection.immutable.Map

import shark.SharkEnv
import shark.collections.Conversions._
import spark.RDD


/**
 * A trait for fetching the map-join hash tables on slaves. Some implementations
 * include using Spark's broadcast variable, or write the hash tables to HDFS
 * and read them back in on the slave nodes.
 */
trait MapJoinHashTablesFetcher {
  def get: Map[Int, MapJoinHashTable]
}


/**
 * Uses Spark's broadcast variable to fetch the hash tables on slaves.
 */
class MapJoinHashTablesBroadcast(hashtables: Map[Int, MapJoinHashTable])
  extends MapJoinHashTablesFetcher with Serializable
{
  val broadcast = SharkEnv.sc.broadcast(hashtables) 
  def get: Map[Int, MapJoinHashTable] = broadcast.value
}

