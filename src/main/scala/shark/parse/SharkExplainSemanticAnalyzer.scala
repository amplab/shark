package shark.parse

import java.io.Serializable
import java.util.ArrayList
import java.util.List

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.ExplainWork

import shark.execution.SharkExplainWork


class SharkExplainSemanticAnalyzer(conf: HiveConf) extends ExplainSemanticAnalyzer(conf) {

  var sem: BaseSemanticAnalyzer = null

  /**
   * This is basically the same as Hive's except we invoke
   * SharkSemanticAnalyzerFactory. We need to do this to get
   * SharkSemanticAnalyzer for SELECT and CTAS queries.
   */
  override def analyzeInternal(ast: ASTNode): Unit = {
    ctx.setExplain(true)
    
    // Create a semantic analyzer for the query
    val childNode = ast.getChild(0).asInstanceOf[ASTNode]
    sem = SharkSemanticAnalyzerFactory.get(conf, childNode)
    sem.analyze(childNode, ctx)

    val extended = (ast.getChildCount() > 1)

    ctx.setResFile(new Path(ctx.getLocalTmpFileURI()))
    var tasks = sem.getRootTasks()
    val fetchTask = sem.getFetchTask()
    if (tasks == null) {
      if (fetchTask != null) {
        tasks = new ArrayList[Task[_ <: Serializable]]();
        tasks.add(fetchTask)
      }
    } else if (fetchTask != null) {
      tasks.add(fetchTask)
    }

    val task = TaskFactory.get(
      new SharkExplainWork(ctx.getResFile().toString(), tasks, childNode.toStringTree(), extended),
      conf)

    rootTasks.add(task)
  }
}

