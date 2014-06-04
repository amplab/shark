package shark

import java.util.ArrayList
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hive.service.cli.TableSchema
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.hive.CatalystContext
import scala.collection.JavaConversions._
import org.apache.commons.lang.exception.ExceptionUtils

class CatalystDriver(hconf: HiveConf) extends Driver with LogHelper {
  private val context: CatalystContext = CatalystEnv.cc
  private var tschema: TableSchema = _
  private var result: (Int, Seq[String], Throwable) = _
  
  override def init(): Unit = {
  }
  
  override def run(command: String): CommandProcessorResponse = {
    val execution = new context.HiveQLQueryExecution(command)

    // TODO unify the error code
    try {
      result = execution.result
      tschema = execution.getResultSetSchema
      
      if(result._1 != 0) {
        logError(s"Failed in [$command]", result._3)
        new CommandProcessorResponse(result._1, ExceptionUtils.getFullStackTrace(result._3), null)
      } else {
        new CommandProcessorResponse(result._1)
      }
    } catch {
      case t: Throwable => 
        logError(s"Failed in [$command]", t)
        new CommandProcessorResponse(-3, ExceptionUtils.getFullStackTrace(t), null)
    }
  }
  
  override def close(): Int = {
    result = null
    tschema = null
    
    0
  }

  /**
   * Get the result schema, currently CatalystDriver doesn't support it yet.
   * TODO: the TableSchema (org.apache.hive.service.cli.TableSchema) is returned by Catalyst, 
   * however, the Driver requires the Schema (org.apache.hadoop.hive.metastore.api.Schema)
   * Need to figure out how to convert the previous to later.
   */
  override def getSchema(): Schema = throw new UnsupportedOperationException("for getSchema")
  def getTableSchema = tschema
  
  override def getResults(res: ArrayList[String]): Boolean = {
    if(result == null) {
      false
    } else {
      res.addAll(result._2)
      result = null
      true
    }
  }
  
  override def destroy() {
    result = null
    tschema = null
  }
}