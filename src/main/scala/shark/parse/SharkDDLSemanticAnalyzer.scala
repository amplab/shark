package shark.parse

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer
import shark.LogHelper
import org.apache.hadoop.hive.ql.parse.HiveParser
import shark.SharkEnv
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer

class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(node: ASTNode): Unit = {
    super.analyzeInternal(node)
    //handle drop table query
    if (node.getToken().getType() == HiveParser.TOK_DROPTABLE) {
      SharkEnv.removeRDD(getTableName(node))
    } else if(node.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS) {
      
    }
  }
  
  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}