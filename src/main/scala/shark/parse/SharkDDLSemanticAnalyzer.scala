package shark.parse

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.{LogHelper, SharkEnv}


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(node: ASTNode): Unit = {
    super.analyzeInternal(node)
    //handle drop table query
    if (node.getToken().getType() == HiveParser.TOK_DROPTABLE) {
      SharkEnv.unpersist(getTableName(node))
    }
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}