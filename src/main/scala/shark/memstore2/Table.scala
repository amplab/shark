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

import org.apache.spark.storage.StorageLevel


/**
 * A container for table metadata managed by Shark and Spark. Subclasses are responsible for
 * how RDDs are set, stored, and accessed.
 *
 * @param tableName Name of this table.
 * @param cacheMode Type of memory storage used for the table (e.g., the Spark block manager).
 * @param preferredStorageLevel The user-specified storage level for the Shark table's RDD(s).
 *     This can be different from the actual RDD storage levels at any point in time, depending on
 *     the the Spark block manager's RDD eviction policy and, for partitioned tables, the
 *     Hive-partition RDD eviction policy.
 */
private[shark] abstract class Table(
    var tableName: String,
    var cacheMode: CacheType.CacheType,
    var preferredStorageLevel: StorageLevel)
