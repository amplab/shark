package shark.server

import java.lang.reflect.Constructor
import java.util.{Map => JMap}
import org.apache.hive.service.cli.session.HiveSession

object SharkExecuteStatementOperation {
  def newExecuteStatementOperation(parentSession: HiveSession,
                                   statement: String,
                                   confOverlay: JMap[String, String])
                                   : Any = {
    val tokens = statement.trim().split("\\s+")
    val command = tokens{0}.toLowerCase
    command match {
      case "set" => {
        val ctor = accessCtor("org.apache.hive.service.cli.operation.SetOperation")
        ctor.newInstance(parentSession, statement, confOverlay)
      }
      case "dfs" => {
        val ctor = accessCtor("org.apache.hive.service.cli.operation.DfsOperation")
        ctor.newInstance(parentSession, statement, confOverlay)
      }
      case "add" => {
        val ctor = accessCtor("org.apache.hive.service.cli.operation.AddResourceOperation")
        ctor.newInstance(parentSession, statement, confOverlay)
      }
      case "delete" => {
        val ctor = accessCtor("org.apache.hive.service.cli.operation.DeleteResourceOperation")
        ctor.newInstance(parentSession, statement, confOverlay)
      }
      case _ => {
        new SharkSQLOperation(parentSession, statement, confOverlay)
      }
    }
  }

  def accessCtor(className : String) : Constructor[_] =  {
    val setClass =  Class.forName(className)
    val setConst =
      setClass.getDeclaredConstructor(
        classOf[HiveSession],
        classOf[String],
        classOf[JMap[String, String]])
    setConst.setAccessible(true)
    setConst
  }
}