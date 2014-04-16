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

import java.util.{HashMap => JavaHashMap, HashSet => JavaHashSet}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.TaskFactory
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}
import org.apache.hadoop.hive.ql.plan.{AlterTableDesc, DDLWork}
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes

import shark.{LogHelper, SharkEnv}
import shark.execution.{SharkDDLWork, SparkLoadWork}
import shark.memstore2.{CacheType, MemoryMetadataManager, OffHeapStorageClient, SharkTblProperties}

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
   *   If the value evaluated by CacheType#shouldCache() is `true`, then create a SparkLoadTask to
   *   load the Hive table into memory.
   *   Set it as a dependent of the Hive DDLTask. A SharkDDLTask counterpart isn't created because
   *   the HadoopRDD creation and transformation isn't a direct Shark metastore operation
   *   (unlike the other cases handled in SharkDDLSemantiAnalyzer).   *
   *   If 'false', then create a SharkDDLTask that will delete the table entry in the Shark
   *   metastore.
   */
  def analyzeAlterTableProperties(ast: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val tableName = getTableName(ast)
    val hiveTable = db.getTable(databaseName, tableName)
    val newTblProps = getAlterTblDesc().getProps
    val oldTblProps = hiveTable.getParameters

    val oldCacheMode = CacheType.fromString(oldTblProps.get(SharkTblProperties.CACHE_FLAG.varname))
    val newCacheMode = CacheType.fromString(newTblProps.get(SharkTblProperties.CACHE_FLAG.varname))
    val cacheInProgress = Option(oldTblProps.get(SharkTblProperties.CACHE_IN_PROGRESS_FLAG.varname))
      .getOrElse("false").toBoolean

    if (oldCacheMode == newCacheMode) {
      logInfo(s"Table is already cached as '$newCacheMode', not changing.")
      return
    }

    if (cacheInProgress && newCacheMode != CacheType.NONE) {
      logError("A cache command is currently in progress, cannot modify cache property.")
      throw new RuntimeException("Cache command already in progress")
    } else if (cacheInProgress) {
      // Don't error when CacheType.NONE to allow recovery if Shark failed in the middle of caching.
      logWarning("A cache command is currently in progress, uncache behavior may be undefined.")
      newTblProps.put(SharkTblProperties.CACHE_IN_PROGRESS_FLAG.varname, false.toString)
    }

    // Un-cache the table if it's currently cached.
    // TODO(aarondav): Could use the cached copy to re-cache the table in a different storage engine
    oldCacheMode match {
      case CacheType.MEMORY | CacheType.MEMORY_ONLY =>
        SharkEnv.memoryMetadataManager.dropTableFromMemory(db, databaseName, tableName)
      case CacheType.OFFHEAP => {
        val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
        OffHeapStorageClient.client.dropTable(tableKey)
      }
      case CacheType.NONE => // do nothing
    }

    // Create and load the data into the desired cache storage.
    if (newCacheMode != CacheType.NONE) {
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
      newTblProps.put(SharkTblProperties.CACHE_IN_PROGRESS_FLAG.varname, true.toString)
      val sparkLoadWork = new SparkLoadWork(
        databaseName,
        tableName,
        SparkLoadWork.CommandTypes.NEW_ENTRY,
        newCacheMode)
      partSpecsOpt.foreach(partSpecs => sparkLoadWork.partSpecs = partSpecs)
      val loadTask = TaskFactory.get(sparkLoadWork, conf)

      // Create a task to set CACHE_IN_PROGRESS_FLAG to false once the load completes.
      val markCachingCompleteDesc = new AlterTableDesc()
      markCachingCompleteDesc.setOldName(tableName)
      markCachingCompleteDesc.setOp(AlterTableTypes.ADDPROPS)
      markCachingCompleteDesc.setProps(new JavaHashMap(Map(
        SharkTblProperties.CACHE_IN_PROGRESS_FLAG.varname -> false.toString)))
      val markCachingCompleteWork = new DDLWork(
        new JavaHashSet[ReadEntity], new JavaHashSet[WriteEntity], markCachingCompleteDesc)
      val markCachingCompleteTask = TaskFactory.get(markCachingCompleteWork, conf)

      loadTask.addDependentTask(markCachingCompleteTask)
      rootTasks.head.addDependentTask(loadTask)
    }
  }

  def analyzeDropTableOrDropParts(ast: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val tableName = getTableName(ast)
    val hiveTableOpt = Option(db.getTable(databaseName, tableName, false /* throwException */))
    // `hiveTableOpt` can be NONE for a DROP TABLE IF EXISTS command on a nonexistent table.
    hiveTableOpt.foreach { hiveTable =>
      val cacheMode = CacheType.fromString(
        hiveTable.getProperty(SharkTblProperties.CACHE_FLAG.varname))
      // Create a SharkDDLTask only if the table is cached.
      if (CacheType.shouldCache(cacheMode)) {
        // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
        // and DDLWorks that contain DropTableDesc objects.
        for (ddlTask <- rootTasks) {
          val dropTableDesc = ddlTask.getWork.asInstanceOf[DDLWork].getDropTblDesc
          val sharkDDLWork = new SharkDDLWork(dropTableDesc)
          sharkDDLWork.cacheMode = cacheMode
          ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
        }
      }
    }
  }

  def analyzeAlterTableAddParts(ast: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val tableName = getTableName(ast)
    val hiveTable = db.getTable(databaseName, tableName)
    val cacheMode = CacheType.fromString(
      hiveTable.getProperty(SharkTblProperties.CACHE_FLAG.varname))
    // Create a SharkDDLTask only if the table is cached.
    if (CacheType.shouldCache(cacheMode)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain AddPartitionDesc objects.
      for (ddlTask <- rootTasks) {
        val addPartitionDesc = ddlTask.getWork.asInstanceOf[DDLWork].getAddPartitionDesc
        val sharkDDLWork = new SharkDDLWork(addPartitionDesc)
        sharkDDLWork.cacheMode = cacheMode
        ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
      }
    }
  }

  private def analyzeAlterTableRename(astNode: ASTNode) {
    val databaseName = db.getCurrentDatabase()
    val oldTableName = getTableName(astNode)
    val hiveTable = db.getTable(databaseName, oldTableName)
    val cacheMode = CacheType.fromString(hiveTable.getProperty(SharkTblProperties.CACHE_FLAG.varname))
    if (CacheType.shouldCache(cacheMode)) {
      val alterTableDesc = getAlterTblDesc()
      val sharkDDLWork = new SharkDDLWork(alterTableDesc)
      sharkDDLWork.cacheMode = cacheMode
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
