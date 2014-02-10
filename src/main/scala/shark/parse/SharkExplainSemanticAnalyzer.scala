/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.parse

import java.io.Serializable
import java.util.ArrayList

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.parse._

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
      new SharkExplainWork(ctx.getResFile().toString(), tasks, childNode.toStringTree(), 
        sem.getInputs(), extended), conf)

    rootTasks.add(task)
  }
}

