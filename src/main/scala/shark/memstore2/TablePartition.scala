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

import java.nio.ByteBuffer

/**
 * TablePartition contains a whole partition of data in columnar format. It
 * simply contains a list of columns. It should be built using a
 * TablePartitionBuilder.
 */
class TablePartition(val size: Int, val buffers: Array[ByteBuffer]) {

  /**
   * Return an iterator for the partition.
   */
  def iterator = new TablePartitionIterator(this)
}