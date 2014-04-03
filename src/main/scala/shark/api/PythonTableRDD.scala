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

import scala.collection.JavaConversions._

import net.razorvine.pickle.Pickler

import org.apache.spark.api.java.JavaRDD

class PythonTableRDD(
    tableRDD: JavaTableRDD)
  extends JavaRDD[Array[Byte]](tableRDD.rdd.mapPartitions(PythonTableRDD.javaRowToPythonRow)) {
  val schema: java.util.Map[String, Int] = tableRDD.first.colname2indexMap
}

/*
 *  These static methods are to be called by Python to run SQL queries. sql2rdd runs the query and
 *  attempts to convert the JavaTableRDD to a Python compatible RDD (an RDD of ByteArrays
 *  that are pickled Python objects). We map the pickle serializer per partition to convert the Java
 *  objects to python objects, and we return the resulting PythonTableRDD to the caller (presumably
 *  a Python process).
 */
object PythonTableRDD {

  def sql2rdd(sc: JavaSharkContext, cmd: String): PythonTableRDD = {
    new PythonTableRDD(sc.sql2rdd(cmd))
  }

  // Pickle a row of java objects to a row of pickled python objects (byte arrays)
  def javaRowToPythonRow(rows: Iterator[Row]): Iterator[Array[Byte]] = {
    // Pickler is not threadsafe, so we use 1 per partition
    val pickle = new Pickler
    rows.map { r =>
      pickle.dumps(r.toSeq.toArray)
    }
  }
}
