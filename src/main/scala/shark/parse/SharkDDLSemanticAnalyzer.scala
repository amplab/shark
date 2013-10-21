package shark.parse

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.TaskFactory
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}
import org.apache.hadoop.hive.ql.plan.DDLWork

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.execution.SharkDDLWork
import shark.{LogHelper, SharkEnv}
import shark.memstore2.MemoryMetadataManager


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(astNode: ASTNode): Unit = {
    super.analyzeInternal(astNode)

    astNode.getToken.getType match {
      case HiveParser.TOK_DROPTABLE => {
        SharkEnv.unpersist(db.getCurrentDatabase(), getTableName(astNode))
      }
      case HiveParser.TOK_ALTERTABLE_RENAME => {
        analyzeAlterTableRename(astNode)
      }
      case _ => Unit
    }
  }

  private def analyzeAlterTableRename(astNode: ASTNode) {
    val oldTableName = getTableName(astNode)
    if (SharkEnv.memoryMetadataManager.contains(db.getCurrentDatabase(), oldTableName)) {
      val newTableName = BaseSemanticAnalyzer.getUnescapedName(
        astNode.getChild(1).asInstanceOf[ASTNode])

      // Hive's DDLSemanticAnalyzer#AnalyzeInternal() will only populate rootTasks with a DDLTask
      // and DDLWork that contains an AlterTableDesc.
      assert(rootTasks.size == 1)
      val ddlTask = rootTasks.head
      val ddlWork = ddlTask.getWork
      assert(ddlWork.isInstanceOf[DDLWork])

      val alterTableDesc = ddlWork.asInstanceOf[DDLWork].getAlterTblDesc
      val sharkDDLWork = new SharkDDLWork(alterTableDesc)
      ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
    }
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}
