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

import org.apache.hadoop.hive.ql.plan.LimitDesc

import org.apache.spark.rdd.{EmptyRDD, RDD}

import shark.SharkEnv


class LimitOperator extends UnaryOperator[LimitDesc] {

  // Only works on the master program.
  def limit = desc.getLimit()

  override def execute(): RDD[_] = {

    val limitNum = desc.getLimit()

    if (limitNum > 0) {
      // Take limit on each partition.
      val inputRdd = executeParents().head._2
      inputRdd.mapPartitions({ iter => iter.take(limitNum) }, preservesPartitioning = true)
    } else {
      new EmptyRDD(SharkEnv.sc)
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    throw new UnsupportedOperationException("LimitOperator.processPartition()")
  }
}

