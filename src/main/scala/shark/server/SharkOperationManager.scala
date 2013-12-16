package shark.server

import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager, Operation}
import org.apache.hive.service.cli.session.HiveSession
import java.util.{Map => JMap}

class SharkOperationManager extends OperationManager {
 override def newExecuteStatementOperation (parentSession: HiveSession, statement: String, confOverlay: JMap[String, String]) : ExecuteStatementOperation = {
   val executeStatementOperation = SharkExecuteStatementOperation.newExecuteStatementOperation(parentSession, statement, confOverlay)
   val castOp = executeStatementOperation.asInstanceOf[ExecuteStatementOperation]
   addOperation(castOp)
   castOp
 }

}
