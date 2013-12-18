package shark.server

import java.util.{Map => JMap}
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hive.service.cli.{HiveSQLException, OperationState, TableSchema}
import org.apache.hive.service.cli.operation.SQLOperation
import org.apache.hive.service.cli.session.HiveSession
import shark.{SharkDriver, Utils}

class SharkSQLOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String])
  extends SQLOperation(parentSession, statement, confOverlay) {

  private val sdriver = {
    val d = new SharkDriver(getParentSession.getHiveConf)
    d.init()
    d
  }

  override def run() {
    setState(OperationState.RUNNING)
    Utils.setSuperField("driver", sdriver, this)
    var response: Option[CommandProcessorResponse] = None
    sdriver.setTryCount(Integer.MAX_VALUE) //maybe useless?
    var subStatement = ""
    try {
      //duplicate: this is also done when Driver compiles command
      subStatement = new VariableSubstitution().substitute(getParentSession.getHiveConf, statement)
    } catch {
      case e: IllegalStateException => {
        setState(OperationState.ERROR)
        throw new HiveSQLException
      }
    }

    response = Option(sdriver.run(subStatement))
    response match {
      case Some(resp: CommandProcessorResponse) => {
        val code = resp.getResponseCode
        if (code != 0) {
          setState(OperationState.ERROR)
          throw new HiveSQLException("Error while processing statement: "
            + resp.getErrorMessage, resp.getSQLState, code)
        }
      }
      case None => {
        setState(OperationState.ERROR)
        throw new HiveSQLException
      }
    }

    val mResultSchema = sdriver.getSchema
    Utils.setSuperField("mResultSchema", mResultSchema, this)
    if (mResultSchema != null && mResultSchema.isSetFieldSchemas) {
      val resultSchema = new TableSchema(mResultSchema)
      Utils.setSuperField("resultSchema", resultSchema, this)
      setHasResultSet(true)
    } else {
      setHasResultSet(false)
    }
    setState(OperationState.FINISHED)
  }

}
