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

import java.util.ArrayList

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import net.razorvine.pickle.Unpickler

import org.apache.spark.api.python.PythonRDD
import org.apache.spark.rdd.RDD

object PythonRDDTable {

  def apply[T](
      rdd: T, 
      tableName: String, 
      cols: ArrayList[String]): Boolean = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.asInstanceOf[RDD[Array[Byte]]].mapPartitions(PythonRDDTable.unpickle).flatMap(r => r.map(e => e.toSeq))(classTag)
    val first = rddSeq.first()
    val classes: Seq[ClassTag[_]] = first.map(getAnyType(_)).toSeq
    val rddTable = new RDDTableFunctions(rddSeq, classes)
    rddTable.saveAsTable(tableName, cols.toSeq)
  }

  private def getAnyType(x: Any): ClassTag[_] = x match {
    case y: Int => implicitly[ClassTag[Int]]
    case y: Long => implicitly[ClassTag[Long]]
    case y: String => implicitly[ClassTag[String]]
    case y: Float => implicitly[ClassTag[Float]]
    case y: Boolean => implicitly[ClassTag[Boolean]]
  }

  private def unpickle(rows: Iterator[Array[Byte]]): Iterator[Seq[ArrayList[_]]] = {
    // Unpickler is not threadsafe, so we use 1 per partition
    val unpickle = new Unpickler
    rows.map { r =>
      unpickle.loads(r).asInstanceOf[ArrayList[ArrayList[_]]].toSeq
    }
  }

}
