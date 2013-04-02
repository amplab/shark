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

package shark.execution

import java.util.{ List => JavaList }

import org.apache.hadoop.hive.ql.exec.{ ExprNodeEvaluator, JoinUtil }
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.SharkEnv

import spark.RDD


/**
 * A trait for fetching the map-join hash tables on slaves. Some implementations
 * include using Spark's broadcast variable, or write the hash tables to HDFS
 * and read them back in on the slave nodes.
 */
trait MapJoinHashTablesFetcher {
  def get: Map[Int, Map[AnyRef, Seq[AnyRef]]]
}


/**
 * Uses Spark's broadcast variable to fetch the hash tables on slaves.
 */
class MapJoinHashTablesBroadcast(hashtables: Map[Int, Map[AnyRef, Seq[AnyRef]]])
  extends MapJoinHashTablesFetcher with Serializable
{
  val broadcast = SharkEnv.sc.broadcast(hashtables)
  def get: Map[Int, Map[AnyRef, Seq[AnyRef]]] = broadcast.value
}

