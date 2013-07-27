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

import org.apache.hadoop.hive.metastore.api.FieldSchema


class ColumnDesc(val name: String, val dataType: DataTypes.DataType) extends Serializable {

  private[api] def this(hiveSchema: FieldSchema) {
    this(hiveSchema.getName, DataTypes.fromHiveType(hiveSchema.getType))
  }

  override def toString = "ColumnDesc(name: %s, type: %s)".format(name, dataType.name)
}
