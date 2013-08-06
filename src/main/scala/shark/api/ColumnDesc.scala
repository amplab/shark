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

package shark.api

import java.util.{List => JList}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Schema


class ColumnDesc(val name: String, val dataType: DataType) extends Serializable {

  private[shark] def this(hiveSchema: FieldSchema) {
    this(hiveSchema.getName, DataTypes.fromHiveType(hiveSchema.getType))
  }

  override def toString = "ColumnDesc(name: %s, type: %s)".format(name, dataType.name)
}


object ColumnDesc {

  def createSchema(fieldSchemas: JList[FieldSchema]): Array[ColumnDesc] = {
    if (fieldSchemas == null) Array.empty else fieldSchemas.map(new ColumnDesc(_)).toArray
  }

  def createSchema(schema: Schema): Array[ColumnDesc] = {
    if (schema == null) Array.empty else createSchema(schema.getFieldSchemas)
  }
}
