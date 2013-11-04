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

package shark.execution

import org.apache.hadoop.hive.ql.plan.TableDesc
import shark.LogHelper


trait ReduceSinkTableDesc extends LogHelper {
  self: Operator[_ <: HiveDesc] =>

  // Seq(tag, (Key TableDesc, Value TableDesc))
  def keyValueDescs(): Seq[(Int, (TableDesc, TableDesc))] = {
    // get the parent ReduceSinkOperator and sort it by tag
    val reduceSinkOps =
      for (op <- self.parentOperators.toSeq if op.isInstanceOf[ReduceSinkOperator])
        yield op.asInstanceOf[ReduceSinkOperator]

    reduceSinkOps.map(f => (f.getTag, f.getKeyValueTableDescs))
  }
}
