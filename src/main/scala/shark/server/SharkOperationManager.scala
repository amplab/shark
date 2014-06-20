package shark
package server

import java.util.{Map => JMap}
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.hive.service.cli.{HiveSQLException, OperationState, TableSchema}

import org.apache.hive.service.cli.OperationHandle
import org.apache.hive.service.cli.operation.Operation

import org.apache.hive.service.cli.FetchOrientation
import org.apache.hive.service.cli.RowSet
import org.apache.hive.service.cli.TableSchema
import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.sql.hive.HiveContext

import org.apache.hive.service.cli.Row
import org.apache.hive.service.cli.ColumnValue;


import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.plans.logical.NativeCommand

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
class SharkOperationManager(hiveContext: HiveContext) extends OperationManager with Logging {
  val handleToOperation =
    Utils.getSuperField("handleToOperation", this).asInstanceOf[java.util.Map[OperationHandle, Operation]]

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val operation = new ExecuteStatementOperation(parentSession, statement, confOverlay) {
      private var result: SchemaRDD = _
      private var iter: Iterator[SparkRow] = _
      private var dataTypes: Array[DataType] = _

      def close(): Unit = {
        // RDDs will be cleaned automatically upon garbage collection.
        logger.debug("CLOSING")
      }

      def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
        if (!iter.hasNext) {
          new RowSet()
        } else {
          val maxRows = maxRowsL.toInt // Do you really want a row batch larger than Int Max? No.
          var curRow = 0
          var rowSet = new ArrayBuffer[Row](maxRows)

          while (curRow < maxRows && iter.hasNext) {
            val sparkRow = iter.next()
            val row = new Row()
            var curCol = 0

            while (curCol < sparkRow.length) {
              dataTypes(curCol) match {
                case StringType => row.addString(sparkRow(curCol).asInstanceOf[String])
                case IntegerType => row.addColumnValue(ColumnValue.intValue(sparkRow.getInt(curCol)))
                case BooleanType => row.addColumnValue(ColumnValue.booleanValue(sparkRow.getBoolean(curCol)))
                case DoubleType => row.addColumnValue(ColumnValue.doubleValue(sparkRow.getDouble(curCol)))
                case FloatType => row.addColumnValue(ColumnValue.floatValue(sparkRow.getFloat(curCol)))
                case LongType => row.addColumnValue(ColumnValue.longValue(sparkRow.getLong(curCol)))
                case ByteType => row.addColumnValue(ColumnValue.byteValue(sparkRow.getByte(curCol)))
                case ShortType => row.addColumnValue(ColumnValue.intValue(sparkRow.getShort(curCol)))
              }
              curCol += 1
            }
            rowSet += row
            curRow += 1
          }
          new RowSet(rowSet, 0)
        }
      }

      def getResultSetSchema: TableSchema = {
        logger.warn(s"Result Schema: ${result.queryExecution.analyzed.output}")
        if (result.queryExecution.analyzed.output.size == 0) {
          new TableSchema(new FieldSchema("Result", "string", "") :: Nil)
        } else {
          val schema = result.queryExecution.analyzed.output.map { attr =>
            new FieldSchema(attr.name, org.apache.spark.sql.hive.HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
          }
          new TableSchema(schema)
        }
      }

      def run(): Unit = {
        logger.info(s"Running query '$statement'")
        setState(OperationState.RUNNING)
        try {
          result = hiveContext.hql(statement)
          logger.debug(result.queryExecution.toString())
          iter = result.queryExecution.toRdd.toLocalIterator
          dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
          setHasResultSet(true)
        } catch {
          // Actually do need to catch Throwable as some failures don't inherit from Exception and HiveServer will
          // silently swallow them.
          case e: Throwable =>
            logger.error("Error executing query:",e)
            throw new HiveSQLException(e.toString)
        }
        setState(OperationState.FINISHED)
      }
    }

   handleToOperation.put(operation.getHandle, operation)
   operation
  }
}
