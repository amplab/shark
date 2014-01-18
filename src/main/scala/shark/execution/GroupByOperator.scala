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

import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.exec.{ReduceSinkOperator => HiveReduceSinkOperator}


/**
 * Unlike Hive, group by in Shark is split into two different operators:
 * GroupByPostShuffleOperator and GroupByPreShuffleOperator. The pre-shuffle one
 * serves as a combiner on each map partition.
 *
 * These two classes are defined in org.apache.hadoop.hive.ql.exec package
 * (scala files) to get around the problem that some Hive classes are only
 * visibile within that class.
 */
object GroupByOperator {
  
  def isPostShuffle(op: HiveGroupByOperator): Boolean = {
    op.getParentOperators().get(0).isInstanceOf[HiveReduceSinkOperator]
  }
  
}

