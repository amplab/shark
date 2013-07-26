package shark.parse

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}
import shark.{LogHelper, SharkEnv}
import spark.RDD
import spark.rdd.UnionRDD

class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(node: ASTNode): Unit = {
    super.analyzeInternal(node)
    //handle drop table query
    if (node.getToken().getType() == HiveParser.TOK_DROPTABLE) {
      val tableName = getTableName(node)

      if (SharkEnv.tachyonUtil.tachyonEnabled()) {
        if (SharkEnv.tachyonUtil.tableExists(tableName)) {
          logInfo("Table " + tableName + " is in Tachyon.");
          if (SharkEnv.tachyonUtil.dropTable(tableName)) {
            logInfo("In Tachyon Table " + tableName + " was deleted.");
          }
        }
      } else {
        logInfo("Tachyon is not enabled. Potential table in it is not dropped.");
      }

      SharkEnv.unpersist(tableName)
    }
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}