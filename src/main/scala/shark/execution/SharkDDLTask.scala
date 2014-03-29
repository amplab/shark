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

package shark.execution

import java.util.{List => JavaList, Map => JavaMap}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask}
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.plan.api.StageType

import org.apache.spark.rdd.EmptyRDD

import shark.{LogHelper, SharkEnv}
import shark.memstore2._
import shark.util.HiveUtils

private[shark] class SharkDDLWork(val ddlDesc: DDLDesc) extends java.io.Serializable {

  var cacheMode: CacheType.CacheType = _

}

/**
 * A task used for Shark-specific metastore operations needed for DDL commands, in addition to the
 * metastore updates done by Hive's DDLTask.
 *
 * Validity checks for DDL commands, such as whether a target table for a CREATE TABLE command
 * already exists, is not done by SharkDDLTask. Instead, the SharkDDLTask is meant to be used as
 * a dependent task of Hive's DDLTask, which handles all error checking. This way, a SharkDDLTask
 * is executed only if the Hive DDLTask is successfully executed - i.e., the DDL statement is a
 * valid one.
 */
private[shark] class SharkDDLTask extends HiveTask[SharkDDLWork]
  with Serializable with LogHelper {

  override def execute(driverContext: DriverContext): Int = {
    val hiveDb = Hive.get(conf)

    // TODO(harvey): Check whether the `hiveDb` is needed. HiveTask should already have a `db` to
    //   use.
    work.ddlDesc match {
      case creatTblDesc: CreateTableDesc => createTable(hiveDb, creatTblDesc, work.cacheMode)
      case addPartitionDesc: AddPartitionDesc => addPartition(hiveDb, addPartitionDesc, work.cacheMode)
      case dropTableDesc: DropTableDesc => dropTableOrPartition(hiveDb, dropTableDesc, work.cacheMode)
      case alterTableDesc: AlterTableDesc => alterTable(hiveDb, alterTableDesc, work.cacheMode)
      case _ => {
        throw new UnsupportedOperationException(
          "Shark does not require a Shark DDL task for: " + work.ddlDesc.getClass.getName)
      }
    }

    // Hive's task runner expects a '0' return value to indicate success, and an exception
    // otherwise
    return 0
  }

  /**
   * Updates Shark metastore for a CREATE TABLE or CTAS command.
   *
   * @param hiveMetadataDb Namespace of the table to create.
   * @param createTblDesc Hive metadata object that contains fields needed to create a Shark Table
   *        entry.
   * @param cacheMode How the created table should be stored and maintained (e.g, MEMORY means that
   *        table data will be in memory and persistent across Shark sessions).
   */
  def createTable(
      hiveMetadataDb: Hive,
      createTblDesc: CreateTableDesc,
      cacheMode: CacheType.CacheType) {
    val dbName = hiveMetadataDb.getCurrentDatabase
    val tableName = createTblDesc.getTableName
    val tblProps = createTblDesc.getTblProps

    if (cacheMode == CacheType.OFFHEAP) {
      // For off-heap tables (partitioned or not), just create the parent directory.
      OffHeapStorageClient.client.createTablePartition(
        MemoryMetadataManager.makeTableKey(dbName, tableName), hivePartitionKeyOpt = None)
    } else {
      val isHivePartitioned = (createTblDesc.getPartCols.size > 0)
      if (isHivePartitioned) {
        // Add a new PartitionedMemoryTable entry in the Shark metastore.
        // An empty table has a PartitionedMemoryTable entry with no 'hivePartition -> RDD' mappings.
        SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
          dbName, tableName, cacheMode, tblProps)
      } else {
        val memoryTable = SharkEnv.memoryMetadataManager.createMemoryTable(
          dbName, tableName, cacheMode)
        // An empty table has a MemoryTable table entry with 'tableRDD' referencing an EmptyRDD.
        memoryTable.put(new EmptyRDD(SharkEnv.sc))
      }
    }
  }

  /**
   * Updates Shark metastore for an ALTER TABLE ADD PARTITION command.
   *
   * @param hiveMetadataDb Namespace of the table to update.
   * @param addPartitionDesc Hive metadata object that contains fields about the new partition.
   */
  def addPartition(
      hiveMetadataDb: Hive,
      addPartitionDesc: AddPartitionDesc,
      cacheMode: CacheType.CacheType) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    val tableName = addPartitionDesc.getTableName

    // Find the set of partition column values that specifies the partition being added.
    val hiveTable = db.getTable(tableName, false /* throwException */);
    val partCols: Seq[String] = hiveTable.getPartCols.map(_.getName)
    val partColToValue: JavaMap[String, String] = addPartitionDesc.getPartSpec
    // String format for partition key: 'col1=value1/col2=value2/...'
    val partKeyStr: String = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partColToValue)
    if (cacheMode == CacheType.OFFHEAP) {
      OffHeapStorageClient.client.createTablePartition(
        MemoryMetadataManager.makeTableKey(dbName, tableName), Some(partKeyStr))
    } else {
      val partitionedTable = getPartitionedTableWithAssertions(dbName, tableName)
      partitionedTable.putPartition(partKeyStr, new EmptyRDD(SharkEnv.sc))
    }
  }

  /**
   * Updates Shark metastore when dropping a table or partition.
   *
   * @param hiveMetadataDb Namespace of the table to drop, or the table that a partition belongs to.
   * @param dropTableDesc Hive metadata object used for both dropping entire tables
   *                      (i.e., DROP TABLE) and for dropping individual partitions of a table
   *                      (i.e., ALTER TABLE DROP PARTITION).
   */
  def dropTableOrPartition(
      hiveMetadataDb: Hive,
      dropTableDesc: DropTableDesc,
      cacheMode: CacheType.CacheType) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    val tableName = dropTableDesc.getTableName
    val hiveTable = db.getTable(tableName, false /* throwException */);
    val partSpecs: JavaList[PartitionSpec] = dropTableDesc.getPartSpecs
    val tableKey = MemoryMetadataManager.makeTableKey(dbName, tableName)

    if (partSpecs == null) {
      // The command is a true DROP TABLE.
      if (cacheMode == CacheType.OFFHEAP) {
        OffHeapStorageClient.client.dropTable(tableKey)
      } else {
        SharkEnv.memoryMetadataManager.removeTable(dbName, tableName)
      }
    } else {
      // The command is an ALTER TABLE DROP PARTITION
      // Find the set of partition column values that specifies the partition being dropped.
      val partCols: Seq[String] = hiveTable.getPartCols.map(_.getName)
      for (partSpec <- partSpecs) {
        val partColToValue: JavaMap[String, String] = partSpec.getPartSpecWithoutOperator
        // String format for partition key: 'col1=value1/col2=value2/...'
        val partKeyStr = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partColToValue)
        if (cacheMode == CacheType.OFFHEAP) {
          OffHeapStorageClient.client.dropTablePartition(tableKey, Some(partKeyStr))
        } else {
          val partitionedTable = getPartitionedTableWithAssertions(dbName, tableName)
          getPartitionedTableWithAssertions(dbName, tableName).removePartition(partKeyStr)
        }
      }
    }
  }

  /**
   * Updates Shark metastore for miscellaneous ALTER TABLE commands.
   *
   * @param hiveMetadataDb Namespace of the table to update.
   * @param alterTableDesc Hive metadata object containing fields needed to handle various table
   *        update commands, such as ALTER TABLE <table> RENAME TO.
   *
   */
  def alterTable(
      hiveMetadataDb: Hive,
      alterTableDesc: AlterTableDesc,
      cacheMode: CacheType.CacheType) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    alterTableDesc.getOp() match {
      case AlterTableDesc.AlterTableTypes.RENAME => {
        val oldName = alterTableDesc.getOldName
        val newName = alterTableDesc.getNewName
        if (cacheMode == CacheType.OFFHEAP) {
          val oldTableKey = MemoryMetadataManager.makeTableKey(dbName, oldName)
          val newTableKey = MemoryMetadataManager.makeTableKey(dbName, newName)
          OffHeapStorageClient.client.renameTable(oldTableKey, newTableKey)
        } else {
          SharkEnv.memoryMetadataManager.renameTable(dbName, oldName, newName)
        }
      }
      case _ => {
        // TODO(harvey): Support more ALTER TABLE commands, such as ALTER TABLE PARTITION RENAME TO.
        throw new UnsupportedOperationException(
          "Shark only requires a Shark DDL task for ALTER TABLE RENAME")
      }
    }
  }

  private def getPartitionedTableWithAssertions(
      dbName: String,
      tableName: String): PartitionedMemoryTable = {
    // Sanity checks: make sure that the table we're modifying exists in the Shark metastore and
    // is actually partitioned.
    val tableOpt = SharkEnv.memoryMetadataManager.getTable(dbName, tableName)
    assert(tableOpt.isDefined, "Internal Error: table %s doesn't exist in Shark metastore.")
    assert(tableOpt.get.isInstanceOf[PartitionedMemoryTable],
      "Internal Error: table %s isn't partitioned when it should be.")
    return tableOpt.get.asInstanceOf[PartitionedMemoryTable]
  }

  override def getType = StageType.DDL

  override def getName = "DDL-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}
