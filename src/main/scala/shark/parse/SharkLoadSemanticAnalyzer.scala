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

import scala.collection.JavaConversions._
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{CopyTask, MoveTask, TaskFactory}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table => HiveTable}
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, LoadSemanticAnalyzer}
import org.apache.hadoop.hive.ql.plan._
import shark.execution.SparkLoadWork
import shark.{LogHelper, SharkEnv}

class SharkLoadSemanticAnalyzer(conf: HiveConf) extends LoadSemanticAnalyzer(conf) {
  
  override def analyzeInternal(ast: ASTNode): Unit = {
    // Delegate to the LoadSemanticAnalyzer parent for error checking the source path formatting.
    super.analyzeInternal(ast)

    // Children of the AST root created for a LOAD DATA [LOCAL] INPATH ... statement are, in order:
    // 1. node containing the path specified by INPATH.
    // 2. internal TOK_TABNAME node that contains the table's name.
    // 3. (optional) node representing the LOCAL modifier.
    val tableASTNode = ast.getChild(1).asInstanceOf[ASTNode]
    val tableName = getTableName(tableASTNode)
    val databaseName = db.getCurrentDatabase()

    val tableOpt = SharkEnv.memoryMetadataManager.getTable(databaseName, tableName)
    if (tableOpt.exists(_.unifyView)) {
      // Find the arguments needed to instantiate a SparkLoadWork.
      val tableSpec = new BaseSemanticAnalyzer.tableSpec(db, conf, tableASTNode)
      val hiveTable = tableSpec.tableHandle
      val moveTask = getMoveTask()
      val partSpecOpt = Option(tableSpec.getPartSpec)
      val isOverwrite = moveTask.getWork.getLoadTableWork.getReplace
      val sparkLoadWork = SparkLoadWork(db, conf, hiveTable, partSpecOpt, isOverwrite)

      // Create a SparkLoadTask that will use a HadoopRDD to read from the source directory. Set it
      // to be a dependent task of the LoadTask so that the SparkLoadTask is executed only if the
      // Hive task executes successfully.
      moveTask.addDependentTask(TaskFactory.get(sparkLoadWork, conf))
    }
  }

  private def getMoveTask(): MoveTask = {
    assert(rootTasks.size == 1)

    // If the execution is local, a CopyTask will be the root task, with a MoveTask child.
    // Otherwise, a MoveTask will be the root.
    var rootTask = rootTasks.head
    val moveTask = if (rootTask.isInstanceOf[CopyTask]) {
      val firstChildTask = rootTask.getChildTasks.head
      assert(firstChildTask.isInstanceOf[MoveTask])
      firstChildTask
    } else {
      rootTask
    }

    // In Hive, LoadTableDesc is referred to as LoadTableWork...
    moveTask.asInstanceOf[MoveTask]
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}

