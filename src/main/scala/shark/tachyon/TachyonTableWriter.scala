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

package shark.tachyon

import java.nio.ByteBuffer


abstract class TachyonTableWriter extends Serializable {

  /** Create a table in Tachyon. Called only on the driver node. */
  def createTable(metadata: ByteBuffer)

  /** Update the metadata in Tachyon. Called only on the driver node. */
  def updateMetadata(metadata: ByteBuffer)

  /** Write the data of a partition of a given column to Tachyon. Called only on worker nodes. */
  def writeColumnPartition(column: Int, part: Int, data: ByteBuffer)
}
