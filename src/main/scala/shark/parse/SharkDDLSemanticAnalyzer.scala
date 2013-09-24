package shark.parse

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}
import org.apache.hadoop.hive.ql.plan.DDLWork

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.execution.EmptyRDD
import shark.{LogHelper, SharkEnv}
import shark.memstore2.MemoryMetadataManager


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(ast: ASTNode): Unit = {
    super.analyzeInternal(ast)

    ast.getToken.getType match {
      case HiveParser.TOK_DROPTABLE => {
        SharkEnv.unpersist(getTableName(ast))
      }
      // Handle ALTER TABLE for cached, Hive-partitioned tables
      case HiveParser.TOK_ALTERTABLE_ADDPARTS => {
        alterTableAddParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_DROPPARTS => {
        alterTableDropParts(ast)
      }
      case _ => Unit
    }
  }

  def alterTableAddParts(ast: ASTNode) {
    val tableName = getTableName(ast)
    val table = db.getTable(db.getCurrentDatabase(), tableName, false /* throwException */);
    val partitionColumns = table.getPartCols.map(_.getName)
    if (SharkEnv.memoryMetadataManager.contains(tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain AddPartitionDesc objects.
      val addPartitionDescs = rootTasks.map(_.getWork.asInstanceOf[DDLWork].getAddPartitionDesc)

      for (addPartitionDesc <- addPartitionDescs) {
        val partitionColumnToValue = addPartitionDesc.getPartSpec
        val keyStr = MemoryMetadataManager.makeHivePartitionKeyStr(
          partitionColumns, partitionColumnToValue)
        SharkEnv.memoryMetadataManager.putHivePartition(tableName, keyStr, new EmptyRDD(SharkEnv.sc))
      }
    }
  }

  def alterTableDropParts(ast: ASTNode) {
    val tableName = getTableName(ast)
    val table = db.getTable(db.getCurrentDatabase(), tableName, false /* throwException */);
    val partitionColumns = table.getPartCols.map(_.getName)
    if (SharkEnv.memoryMetadataManager.contains(tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with a DDLTask
      // and a DDLWork that contains a DropTableDesc object.
      val partSpecs = rootTasks.map(
        _.getWork.asInstanceOf[DDLWork].getDropTblDesc).head.getPartSpecs
      for (partSpec <- partSpecs) {
        val partitionColumnToValue = partSpec.getPartSpecWithoutOperator
        val keyStr = MemoryMetadataManager.makeHivePartitionKeyStr(
          partitionColumns, partitionColumnToValue)
        SharkEnv.memoryMetadataManager.putHivePartition(tableName, keyStr, new EmptyRDD(SharkEnv.sc))
      }
    }
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}
