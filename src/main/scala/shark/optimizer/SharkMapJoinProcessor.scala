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

package shark.optimizer

import java.util.{LinkedHashMap => JavaLinkedHashMap}

import org.apache.hadoop.hive.ql.exec.{MapJoinOperator, JoinOperator, Operator}
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor
import org.apache.hadoop.hive.ql.parse.{ParseContext, QBJoinTree, OpParseContext}
import org.apache.hadoop.hive.ql.plan.OperatorDesc
import org.apache.hadoop.hive.conf.HiveConf

class SharkMapJoinProcessor extends MapJoinProcessor {

  /**
   * Override generateMapJoinOperator to bypass the step of validating Map Join hints int Hive.
   */
  override def generateMapJoinOperator(
      pctx: ParseContext,
      op: JoinOperator,
      joinTree: QBJoinTree,
      mapJoinPos: Int): MapJoinOperator = {
    val hiveConf: HiveConf = pctx.getConf
    val noCheckOuterJoin: Boolean =
      HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN) &&
      HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN)

    val opParseCtxMap: JavaLinkedHashMap[Operator[_ <: OperatorDesc], OpParseContext] =
      pctx.getOpParseCtx

    // Explicitly set validateMapJoinTree to false to bypass the step of validating
    // Map Join hints in Hive.
    val validateMapJoinTree = false
    val mapJoinOp: MapJoinOperator =
      MapJoinProcessor.convertMapJoin(
        opParseCtxMap, op, joinTree, mapJoinPos, noCheckOuterJoin, validateMapJoinTree)

    // Hive originally uses genSelectPlan to insert an dummy select after the MapJoinOperator.
    // We should not need this step.
    // create a dummy select to select all columns
    // MapJoinProcessor.genSelectPlan(pctx, mapJoinOp)

    return mapJoinOp
  }
}
