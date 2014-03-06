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

package shark.execution.cg


class CGAssertRuntimeException(message: String = null, cause: Throwable = null) 
  extends RuntimeException(message, cause)

import shark.execution.cg.row.CGField
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
class CGNotSupportDataTypeRuntimeException(message: String = null) 
  extends RuntimeException(message, null) {
  
  def this(t: CGField[_]) {
    this("Not support the data type:" + t.oi)
  }
  
  def this(t: TypeInfo) {
    this("Not support the data type:" + t.getTypeName())
  }
}