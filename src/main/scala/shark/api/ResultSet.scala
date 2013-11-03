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

import java.util.{Arrays, Collections, List => JList}


class ResultSet private[shark](_schema: Array[ColumnDesc], _results: Array[Array[Object]]) {

  /**
   * The schema for the query results, for use in Scala.
   */
  def schema: Seq[ColumnDesc] = _schema.toSeq

  /**
   * Query results, for use in Scala.
   */
  def results: Seq[Array[Object]] = _results.toSeq

  /**
   * Get the schema for the query results as an immutable list, for use in Java.
   */
  def getSchema: JList[ColumnDesc] = Collections.unmodifiableList(Arrays.asList(_schema : _*))

  /**
   * Get the query results as an immutable list, for use in Java.
   */
  def getResults: JList[Array[Object]] = Collections.unmodifiableList(Arrays.asList(_results : _*))

  override def toString: String = {
    "ResultSet(" + _schema.map(c => c.name + " " + c.dataType).mkString("\t") + ")\n" +
    _results.map(row => row.mkString("\t")).mkString("\n")
  }

}
