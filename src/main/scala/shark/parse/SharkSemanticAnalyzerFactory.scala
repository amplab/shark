package shark.parse

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, SemanticAnalyzerFactory,
  ExplainSemanticAnalyzer, SemanticAnalyzer}

import shark.SharkConfVars


object SharkSemanticAnalyzerFactory {

  /**
   * Return a semantic analyzer for the given ASTNode.
   */
  def get(conf: HiveConf, tree:ASTNode): BaseSemanticAnalyzer = {
    val baseSem = SemanticAnalyzerFactory.get(conf, tree)

    if (baseSem.isInstanceOf[SemanticAnalyzer]) {
      new SharkSemanticAnalyzer(conf)
    } else if (baseSem.isInstanceOf[ExplainSemanticAnalyzer] &&
        SharkConfVars.getVar(conf, SharkConfVars.EXPLAIN_MODE) == "shark") {
      new SharkExplainSemanticAnalyzer(conf)
    } else {
      baseSem
    }
  }
}

