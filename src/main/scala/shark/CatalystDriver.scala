package shark

import java.util.ArrayList
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.sql.hive.CatalystContext

import scala.collection.JavaConversions._

class CatalystDriver(hconf: HiveConf) extends Driver {
  private val context: CatalystContext = CatalystEnv.cc
  private var schema: Schema = _
  private var result: (Int, Seq[String]) = _
  
  override def init(): Unit = {
  }
  
  override def run(command: String): CommandProcessorResponse = {
    this.result = new context.HiveQLQueryExecution(command).result
    
    new CommandProcessorResponse(this.result._1)
  }
  
  override def close(): Int = {
    result = null
    schema = null
    
    0
  }
  
  override def getSchema(): Schema = schema
  
  override def getResults(res: ArrayList[String]): Boolean = {
    if(result == null) {
      false
    } else {
      res.addAll(result._2)
      true
    }
  }
  
  override def destroy() {
    result = null
    schema = null
  }
}