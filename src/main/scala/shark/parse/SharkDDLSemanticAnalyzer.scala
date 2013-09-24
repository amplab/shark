package shark.parse

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.TaskFactory
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}
import org.apache.hadoop.hive.ql.plan.DDLWork

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.execution.{EmptyRDD, SparkDDLWork}
import shark.{LogHelper, SharkEnv}
import shark.memstore2.MemoryMetadataManager


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(ast: ASTNode): Unit = {
    super.analyzeInternal(ast)

    ast.getToken.getType match {
      case HiveParser.TOK_DROPTABLE => {
        // TODO(harvey): Execute this in SparkDDLTask. This somewhat works right now because
        //               unpersist() returns silently when the table doesn't exist. However, it
        //               ignores any drop protections.
        SharkEnv.unpersist(getTableName(ast))
      }
      // Handle ALTER TABLE for cached, Hive-partitioned tables
      case HiveParser.TOK_ALTERTABLE_ADDPARTS => {
        analyzeAlterTableAddParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_DROPPARTS => {
        alterTableDropParts(ast)
      }
      case _ => Unit
    }
  }

  def analyzeAlterTableAddParts(ast: ASTNode) {
    val tableName = getTableName(ast)
    // Create a SparkDDLTask only if the table is cached.
    if (SharkEnv.memoryMetadataManager.contains(tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain AddPartitionDesc objects.
      for (ddlTask <- rootTasks) {
        val addPartitionDesc = ddlTask.getWork.asInstanceOf[DDLWork].getAddPartitionDesc
        val sparkDDLWork = new SparkDDLWork(addPartitionDesc)
        ddlTask.addDependentTask(TaskFactory.get(sparkDDLWork, conf))
      }
    }
  }

  def alterTableDropParts(ast: ASTNode) {
    val tableName = getTableName(ast)
    // Create a SparkDDLTask only if the table is cached.
    if (SharkEnv.memoryMetadataManager.contains(tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain AddPartitionDesc objects.
      for (ddlTask <- rootTasks) {
        val dropTableDesc = ddlTask.getWork.asInstanceOf[DDLWork].getDropTblDesc
        val sparkDDLWork = new SparkDDLWork(dropTableDesc)
        ddlTask.addDependentTask(TaskFactory.get(sparkDDLWork, conf))
      }
    }
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}
