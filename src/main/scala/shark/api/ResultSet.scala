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