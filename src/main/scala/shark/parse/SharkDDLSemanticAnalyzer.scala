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

import java.util.{HashMap => JavaHashMap}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.TaskFactory
import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.HiveParser
import org.apache.hadoop.hive.ql.parse.SemanticException
import org.apache.hadoop.hive.ql.plan.{AlterTableDesc, DDLWork}

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.{LogHelper, SharkEnv}
import shark.execution.{SharkDDLWork, SparkLoadWork}
import shark.memstore2.{CacheType, MemoryMetadataManager, SharkTblProperties}


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(ast: ASTNode): Unit = {
    super.analyzeInternal(ast)

    ast.getToken.getType match {
      case HiveParser.TOK_ALTERTABLE_ADDPARTS => {
        analyzeAlterTableAddParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_DROPPARTS => {
        analyzeDropTableOrDropParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_RENAME => {
        analyzeAlterTableRename(ast)
      }
      case HiveParser.TOK_ALTERTABLE_PROPERTIES => {
        analyzeAlterTableProperties(ast)
      }
      case HiveParser.TOK_DROPTABLE => {
        analyzeDropTableOrDropParts(ast)
      }
      case _ => Unit
    }
  }

  /**
   * Handle table property changes.
   * How Shark-specific changes are handled:
   * - "shark.cache":
   *   If 'true', then create a SparkLoadTask to load the Hive table into memory.
   *   Set it as a dependent of the Hive DDLTask. A SharkDDLTask counterpart isn't created because
   *   the HadoopRDD creation and transformation isn't a direct Shark metastore operation
   *   (unlike the other cases handled in SharkDDLSemantiAnalyzer).   *
   *   If 'false', then create a SharkDDLTask that will delete the table entry in the Shark
   *   metastore.
   *
   * - "shark.cache.unifyView" :
   *   If 'true' and "shark.cache" is true, then the SparkLoadTask created should read this from the
   *   table properties when adding an entry to the Shark metastore.
   *
   *   TODO(harvey): Add this, though reevaluate it too...some Spark RDDs might depend on the old
   *   version of the RDD, so simply dropping it might not work.
   */
  def analyzeAlterTableProperties(ast: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val tableName = getTableName(ast)
    val hiveTable = db.getTable(databaseName, tableName)
    val newTblProps = getAlterTblDesc().getProps
    val oldTblProps = hiveTable.getParameters

    val oldCacheMode = CacheType.fromString(oldTblProps.get(SharkTblProperties.CACHE_FLAG.varname))
    val newCacheMode = CacheType.fromString(newTblProps.get(SharkTblProperties.CACHE_FLAG.varname))
    if (!CacheType.shouldCache(oldCacheMode) && CacheType.shouldCache(newCacheMode)) {
      // The table should be cached (and is not already cached).
      val partSpecsOpt = if (hiveTable.isPartitioned) {
        val columnNames = hiveTable.getPartCols.map(_.getName)
        val partSpecs = db.getPartitions(hiveTable).map { partition =>
          val partSpec = new JavaHashMap[String, String]()
          val values = partition.getValues()
          columnNames.zipWithIndex.map { case(name, index) => partSpec.put(name, values(index)) }
          partSpec
        }
        Some(partSpecs)
      } else {
        None
      }
      newTblProps.put(SharkTblProperties.CACHE_FLAG.varname, newCacheMode.toString)
      val unifyView = SharkTblProperties.getOrSetDefault(newTblProps,
        SharkTblProperties.UNIFY_VIEW_FLAG).toBoolean
      val reloadOnRestart = SharkTblProperties.getOrSetDefault(newTblProps,
        SharkTblProperties.RELOAD_ON_RESTART_FLAG).toBoolean
      val sparkLoadWork = new SparkLoadWork(databaseName, tableName,
        SparkLoadWork.CommandTypes.NEW_ENTRY, newCacheMode)
      sparkLoadWork.unifyView = unifyView
      sparkLoadWork.reloadOnRestart = reloadOnRestart
      partSpecsOpt.foreach(partSpecs => sparkLoadWork.partSpecs = partSpecs)
      rootTasks.head.addDependentTask(TaskFactory.get(sparkLoadWork, conf))
    }
    if (CacheType.shouldCache(oldCacheMode) && !CacheType.shouldCache(newCacheMode)) {
      val isUnifiedView = Option(oldTblProps.get(SharkTblProperties.UNIFY_VIEW_FLAG.varname)).
        exists(_.toBoolean)
      // Uncache the table.
      if (isUnifiedView) {
        SharkEnv.memoryMetadataManager.dropUnifiedView(db, databaseName, tableName)
      } else {
        throw new SemanticException(
          "Only unified views can be uncached. A memory-only table should be dropped.")
      }
    }

  }

  def analyzeDropTableOrDropParts(ast: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val tableName = getTableName(ast)
    // Create a SharkDDLTask only if the table is cached.
    if (SharkEnv.memoryMetadataManager.containsTable(databaseName, tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain DropTableDesc objects.
      for (ddlTask <- rootTasks) {
        val dropTableDesc = ddlTask.getWork.asInstanceOf[DDLWork].getDropTblDesc
        val sharkDDLWork = new SharkDDLWork(dropTableDesc)
        ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
      }
    }
  }

  def analyzeAlterTableAddParts(ast: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val tableName = getTableName(ast)
    // Create a SharkDDLTask only if the table is cached.
    if (SharkEnv.memoryMetadataManager.containsTable(databaseName, tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain AddPartitionDesc objects.
      for (ddlTask <- rootTasks) {
        val addPartitionDesc = ddlTask.getWork.asInstanceOf[DDLWork].getAddPartitionDesc
        val sharkDDLWork = new SharkDDLWork(addPartitionDesc)
        ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
      }
    }
  }

  private def analyzeAlterTableRename(astNode: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val oldTableName = getTableName(astNode)
    if (SharkEnv.memoryMetadataManager.containsTable(databaseName, oldTableName)) {
      val newTableName = BaseSemanticAnalyzer.getUnescapedName(
        astNode.getChild(1).asInstanceOf[ASTNode])
      val alterTableDesc = getAlterTblDesc()
      val sharkDDLWork = new SharkDDLWork(alterTableDesc)
      rootTasks.head.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
    }
  }

  private def getAlterTblDesc(): AlterTableDesc = {
    // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with a DDLTask
    // and DDLWork that contains an AlterTableDesc.
    assert(rootTasks.size == 1)
    val ddlTask = rootTasks.head
    val ddlWork = ddlTask.getWork
    assert(ddlWork.isInstanceOf[DDLWork])
    ddlWork.asInstanceOf[DDLWork].getAlterTblDesc
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}
