package shark.parse

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.{LogHelper, SharkEnv}


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(node: ASTNode): Unit = {
    super.analyzeInternal(node)

    node.getToken.getType match {
      case HiveParser.TOK_DROPTABLE => {
        SharkEnv.unpersist(getTableName(node))
      }
      // Handle ALTER TABLE for cached, Hive-partitioned tables
      case HiveParser.TOK_ALTERTABLE_ADDPARTS => {
        Unit
      }
      case HiveParser.TOK_ALTERTABLE_DROPPARTS => {
        Unit
      }
      case HiveParser.TOK_ALTERTABLE_PARTITION => {
        Unit
      }
      case _ => Unit
    }
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}