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

package shark.memstore2

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * A metadata container for a table in Shark that's backed by an RDD.
 */
private[shark]
class MemoryTable(
    tableName: String,
    cacheMode: CacheType.CacheType,
    preferredStorageLevel: StorageLevel)
  extends Table(tableName, cacheMode, preferredStorageLevel) {

  // RDD that contains the contents of this table.
  private var _tableRDD: RDD[TablePartition] = _

  def tableRDD: RDD[TablePartition] = _tableRDD

  def tableRDD_= (rdd: RDD[TablePartition]) = _tableRDD = rdd

}
