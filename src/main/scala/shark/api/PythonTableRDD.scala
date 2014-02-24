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

package shark.api

import net.razorvine.pickle.Pickler

import scala.collection.JavaConversions._

import org.apache.spark.api.java.JavaRDD

class PythonTableRDD(tableRDD: JavaTableRDD)
extends JavaRDD[Array[Byte]](tableRDD.rdd.mapPartitions(PythonTableRDD.pickle)) {
  val schema: java.util.Map[String, Int] = tableRDD.first.colname2indexMap
}

object PythonTableRDD {

  def sql2rdd(sc: JavaSharkContext, cmd: String): PythonTableRDD = {
    new PythonTableRDD(sc.sql2rdd(cmd))
  }

  def pickle(rows: Iterator[Row]): Iterator[Array[Byte]] = {
    // Pickler is not threadsafe, so we use 1 per partition
    val pickle = new Pickler
    rows.map { r =>
      pickle.dumps(r.toSeq.toArray)
    }
  }
}
