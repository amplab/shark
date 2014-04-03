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

import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.rdd.RDD


class JavaRDDTableFunctions(val rdd: RDD[Seq[_]], val classTags: Seq[ClassTag[_]])
  extends JavaRDDLike[Seq[_], JavaRDDTableFunctions] {

  val RDDTableFunctions = new RDDTableFunctions(rdd, classTags)

  override def wrapRDD(rdd: RDD[Seq[_]]): JavaRDDTableFunctions = new JavaRDDTableFunctions(rdd, classTags)

  // Common RDD functions
  override val classTag: ClassTag[Seq[_]] = implicitly[ClassTag[Seq[_]]]

  def saveAsTable(tableName: String, fields: Seq[String]): Boolean =
    RDDTableFunctions.saveAsTable(tableName, fields)

}
