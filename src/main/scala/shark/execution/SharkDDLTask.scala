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
import shark.memstore2.{CacheType, MemoryMetadataManager, PartitionedMemoryTable}


private[shark] class SharkDDLWork(val ddlDesc: DDLDesc) extends java.io.Serializable {

  // Used only for CREATE TABLE.
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
      case addPartitionDesc: AddPartitionDesc => addPartition(hiveDb, addPartitionDesc)
      case dropTableDesc: DropTableDesc => dropTableOrPartition(hiveDb, dropTableDesc)
      case alterTableDesc: AlterTableDesc => alterTable(hiveDb, alterTableDesc)
      case _ => {
        throw new UnsupportedOperationException(
          "Shark does not require a Shark DDL task for: " + work.ddlDesc.getClass.getName)
      }
    }

    // Hive's task runner expects a '0' return value to indicate success, and an exception
    // otherwise
    return 0
  }

  /** Handles a CREATE TABLE or CTAS. */
  def createTable(
      hiveMetadataDb: Hive,
      createTblDesc: CreateTableDesc, cacheMode: CacheType.CacheType) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    val tableName = createTblDesc.getTableName
    val tblProps = createTblDesc.getTblProps

    val preferredStorageLevel = MemoryMetadataManager.getStorageLevelFromString(
      tblProps.get("shark.cache.storageLevel"))
    val isHivePartitioned = (createTblDesc.getPartCols.size > 0)
    if (isHivePartitioned) {
      // Add a new PartitionedMemoryTable entry in the Shark metastore.
      // An empty table has a PartitionedMemoryTable entry with no 'hivePartition -> RDD' mappings.
      SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
        dbName,
        tableName,
        cacheMode,
        preferredStorageLevel,
        tblProps)
    } else {
      val newTable = SharkEnv.memoryMetadataManager.createMemoryTable(
        dbName, tableName, cacheMode, preferredStorageLevel)
      // An empty table has a MemoryTable table entry with 'tableRDD' referencing an EmptyRDD.
      newTable.tableRDD = new EmptyRDD(SharkEnv.sc)
    }
  }

  /** Handles an ALTER TABLE ADD PARTITION. */
  def addPartition(
      hiveMetadataDb: Hive,
      addPartitionDesc: AddPartitionDesc) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    val tableName = addPartitionDesc.getTableName
    val partitionedTable = getPartitionedTableWithAssertions(dbName, tableName)

    // Find the set of partition column values that specifies the partition being added.
    val hiveTable = db.getTable(tableName, false /* throwException */);
    val partCols: Seq[String] = hiveTable.getPartCols.map(_.getName)
    val partColToValue: JavaMap[String, String] = addPartitionDesc.getPartSpec
    // String format for partition key: 'col1=value1/col2=value2/...'
    val partKeyStr: String = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partColToValue)
    partitionedTable.putPartition(partKeyStr, new EmptyRDD(SharkEnv.sc))
  }

  /**
   * A DropTableDesc is used for both dropping entire tables (i.e., DROP TABLE) and for dropping
   * individual partitions of a table (i.e., ALTER TABLE DROP PARTITION).
   */
  def dropTableOrPartition(
      hiveMetadataDb: Hive,
      dropTableDesc: DropTableDesc) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    val tableName = dropTableDesc.getTableName
    val hiveTable = db.getTable(tableName, false /* throwException */);
    val partSpecs: JavaList[PartitionSpec] = dropTableDesc.getPartSpecs

    if (partSpecs == null) {
      // The command is a true DROP TABLE.
      SharkEnv.dropTable(dbName, tableName)
    } else {
      // The command is an ALTER TABLE DROP PARTITION
      val partitionedTable = getPartitionedTableWithAssertions(dbName, tableName)
      // Find the set of partition column values that specifies the partition being dropped.
      val partCols: Seq[String] = hiveTable.getPartCols.map(_.getName)
      for (partSpec <- partSpecs) {
        val partColToValue: JavaMap[String, String] = partSpec.getPartSpecWithoutOperator
        // String format for partition key: 'col1=value1/col2=value2/...'
        val partKeyStr = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partColToValue)
        partitionedTable.removePartition(partKeyStr)
      }
    }
  }

  /** Handles miscellaneous ALTER TABLE 'tableName' commands. */
  def alterTable(
      hiveMetadataDb: Hive,
      alterTableDesc: AlterTableDesc) {
    val dbName = hiveMetadataDb.getCurrentDatabase()
    alterTableDesc.getOp() match {
      case AlterTableDesc.AlterTableTypes.RENAME => {
        val oldName = alterTableDesc.getOldName
        val newName = alterTableDesc.getNewName
        SharkEnv.memoryMetadataManager.renameTable(dbName, oldName, newName)
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
