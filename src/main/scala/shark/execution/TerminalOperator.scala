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

import java.util.Date

import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.plan.FileSinkDesc


/**
 * File sink operator. It can accomplish one of the three things:
 * - write query output to disk
 * - cache query output
 * - return query as RDD directly (without materializing it)
 */
class TerminalOperator extends UnaryOperator[FileSinkDesc] {

  // Create a local copy of hconf and hiveSinkOp so we can XML serialize it.
  @BeanProperty var localHiveOp: HiveFileSinkOperator = _
  @BeanProperty var localHconf: HiveConf = _
  @BeanProperty val now = new Date()

  override def initializeOnMaster() {
    super.initializeOnMaster()
    localHconf = super.hconf
    // Set parent to null so we won't serialize the entire query plan.
    localHiveOp.setParentOperators(null)
    localHiveOp.setChildOperators(null)
    localHiveOp.setInputObjInspectors(null)
  }

  override def initializeOnSlave() {
    localHiveOp.initialize(localHconf, Array(objectInspector))
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = iter
}


/**
 * Collect the output as a TableRDD.
 */
class TableRddSinkOperator extends TerminalOperator {}
