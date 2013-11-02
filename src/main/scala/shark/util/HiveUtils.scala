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

import java.util.{ArrayList => JavaArrayList, HashSet => JavaHashSet}
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, DDLWork, DropTableDesc}
import org.apache.hadoop.hive.metastore.api.FieldSchema

import shark.api.{DataType, DataTypes}
import org.apache.hadoop.hive.ql.exec.DDLTask
import org.apache.hadoop.hive.conf.HiveConf


private[shark] object HiveUtils {

  def getJavaPrimitiveObjectInspector(m: ClassManifest[_]): PrimitiveObjectInspector = {
    getJavaPrimitiveObjectInspector(DataTypes.fromManifest(m))
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
   * Execute the create table DDL operation against Hive's metastore.
   */
  def createTableInHive(
      tableName: String,
      columnNames: Seq[String],
      columnTypes: Seq[ClassManifest[_]]): Boolean = {
    val schema = columnNames.zip(columnTypes).map { case (colName, manifest) =>
      new FieldSchema(colName, DataTypes.fromManifest(manifest).hiveName, "")
    }

    // Setup the create table descriptor with necessary information.
    val createTbleDesc = new CreateTableDesc()
    createTbleDesc.setTableName(tableName)
    createTbleDesc.setCols(new JavaArrayList[FieldSchema](schema))
    createTbleDesc.setTblProps(Map("shark.cache" -> "heap"))
    createTbleDesc.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
    createTbleDesc.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
    createTbleDesc.setSerName(classOf[shark.memstore2.ColumnarSerDe].getName)
    createTbleDesc.setNumBuckets(-1)

    // Execute the create table against the metastore.
    val ddlWork = new DDLWork(new JavaHashSet[ReadEntity],
                              new JavaHashSet[WriteEntity],
                              createTbleDesc)
    val taskExecutionStatus = executeDDLTaskDirectly(ddlWork)
    taskExecutionStatus == 0
  }

  def dropTableInHive(
      tableName: String): Boolean = {
    // Setup the drop table descriptor with necessary information.
    val dropTblDesc = new DropTableDesc(
      tableName,
      false /* expectView */,
      false /* ifExists */,
      false /* stringPartitionColumns */)

    // Execute the drop table against the metastore.
    val ddlWork = new DDLWork(new JavaHashSet[ReadEntity],
                              new JavaHashSet[WriteEntity],
                              dropTblDesc)
    val taskExecutionStatus = executeDDLTaskDirectly(ddlWork)
    taskExecutionStatus == 0
  }

  def executeDDLTaskDirectly(ddlWork: DDLWork): Int = {
    val task = new DDLTask()
    task.initialize(new HiveConf, null, null)
    task.setWork(ddlWork)

    // Hive returns 0 if the create table command is executed successfully.
    task.execute(null)
  }
}
