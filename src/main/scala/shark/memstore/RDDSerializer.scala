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

package shark.memstore

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Writable


/**
 * Serialize a RDD partition using the given columnar serde.
 * Make sure the serde is initialized before passing it in.
 */
class RDDSerializer(serDe: ColumnarSerDe) {
  // Columnar serialization serializes a partition into two elements. The first
  // element is the number of rows in the partition, and the second element is
  // a ColumnarWritable object containing all rows in columnar format.

  def serialize(iter: Iterator[Any], oi: ObjectInspector): Iterator[_] = {

    var tableStorageBuilder: Writable = null
    iter.foreach { row =>
      tableStorageBuilder = serDe.serialize(row.asInstanceOf[AnyRef], oi)
    }

    if (tableStorageBuilder != null) {
      Iterator(tableStorageBuilder.asInstanceOf[TableStorageBuilder].build)
    } else {
      // This partition is empty.
      Iterator()
    }
  }
}
