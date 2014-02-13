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

package shark.util

import java.util.{Arrays => JArrays, ArrayList => JArrayList}
import java.util.{HashMap => JHashMap, HashSet => JHashSet}
import java.util.Properties

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.ql.exec.DDLTask
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, DDLWork, DropTableDesc}

import shark.api.{DataType, DataTypes}
import shark.memstore2.SharkTblProperties


private[shark] object HiveUtils {

  def getJavaPrimitiveObjectInspector(c: ClassTag[_]): PrimitiveObjectInspector = {
    getJavaPrimitiveObjectInspector(DataTypes.fromClassTag(c))
  }

  def getJavaPrimitiveObjectInspector(t: DataType): PrimitiveObjectInspector = t match {
    case DataTypes.BOOLEAN => PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
    case DataTypes.TINYINT => PrimitiveObjectInspectorFactory.javaByteObjectInspector
    case DataTypes.SMALLINT => PrimitiveObjectInspectorFactory.javaShortObjectInspector
    case DataTypes.INT => PrimitiveObjectInspectorFactory.javaIntObjectInspector
    case DataTypes.BIGINT => PrimitiveObjectInspectorFactory.javaLongObjectInspector
    case DataTypes.FLOAT => PrimitiveObjectInspectorFactory.javaFloatObjectInspector
    case DataTypes.DOUBLE => PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
    case DataTypes.TIMESTAMP => PrimitiveObjectInspectorFactory.javaTimestampObjectInspector
    case DataTypes.STRING => PrimitiveObjectInspectorFactory.javaStringObjectInspector
  }

  /**
   * Returns a StructObjectInspector
   */
  def makeStandardStructObjectInspector(fieldNames: Seq[String], ois: Seq[PrimitiveObjectInspector]) = {
    val fields = fieldNames.toList
    val oiList = ois.toList

    ObjectInspectorFactory.getStandardStructObjectInspector(fields, oiList)
  }

  /**
   * Return a UnionStructObjectInspector that combines the StructObjectInspectors for the table
   * schema and the partition columns, which are virtual in Hive.
   */
  def makeUnionOIForPartitionedTable(
      partProps: Properties,
      tableSerDe: Deserializer): UnionStructObjectInspector = {
    val partCols = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
    val partColNames = new JArrayList[String]
    val partColObjectInspectors = new JArrayList[ObjectInspector]
    partCols.trim().split("/").foreach { colName =>
      partColNames.add(colName)
      partColObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    }

    val partColObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      partColNames, partColObjectInspectors)
    val oiList = JArrays.asList(
      tableSerDe.getObjectInspector.asInstanceOf[StructObjectInspector],
      partColObjectInspector.asInstanceOf[StructObjectInspector])
    // New oi is union of table + partition object inspectors
    ObjectInspectorFactory.getUnionStructObjectInspector(oiList)
  }

  /**
   * Execute the create table DDL operation against Hive's metastore.
   */
  def createTableInHive(
      tableName: String,
      columnNames: Seq[String],
      columnTypes: Seq[ClassTag[_]],
      hiveConf: HiveConf = new HiveConf): Boolean = {
    val schema = columnNames.zip(columnTypes).map { case (colName, classTag) =>
      new FieldSchema(colName, DataTypes.fromClassTag(classTag).hiveName, "")
    }

    // Setup the create table descriptor with necessary information.
    val createTableDesc = new CreateTableDesc()
    createTableDesc.setTableName(tableName)
    createTableDesc.setCols(new JArrayList[FieldSchema](schema))
    createTableDesc.setTblProps(
      SharkTblProperties.initializeWithDefaults(new JHashMap[String, String]()))
    createTableDesc.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
    createTableDesc.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
    createTableDesc.setSerName(classOf[shark.memstore2.ColumnarSerDe].getName)
    createTableDesc.setNumBuckets(-1)

    // Execute the create table against the Hive metastore.
    val work = new DDLWork(new JHashSet[ReadEntity], new JHashSet[WriteEntity], createTableDesc)
    val taskExecutionStatus = executeDDLTaskDirectly(work, hiveConf)
    taskExecutionStatus == 0
  }

  def dropTableInHive(tableName: String, hiveConf: HiveConf = new HiveConf): Boolean = {
    // Setup the drop table descriptor with necessary information.
    val dropTblDesc = new DropTableDesc(
      tableName,
      false /* expectView */,
      false /* ifExists */,
      false /* stringPartitionColumns */)

    // Execute the drop table against the metastore.
    val work = new DDLWork(new JHashSet[ReadEntity], new JHashSet[WriteEntity], dropTblDesc)
    val taskExecutionStatus = executeDDLTaskDirectly(work, hiveConf)
    taskExecutionStatus == 0
  }

  /**
   * Creates a DDLTask from the DDLWork given, and directly calls DDLTask#execute(). Returns 0 if
   * the create table command is executed successfully.
   * This is safe to use for all DDL commands except for AlterTableTypes.ARCHIVE, which actually
   * requires the DriverContext created in Hive Driver#execute().
   */
  def executeDDLTaskDirectly(ddlWork: DDLWork, hiveConf: HiveConf): Int = {
    val task = new DDLTask()
    task.initialize(hiveConf, null /* queryPlan */, null /* ctx: DriverContext */)
    task.setWork(ddlWork)
    task.execute(null /* driverContext */)
  }
}
