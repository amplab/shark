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

import java.util.{HashMap => JHashMap, List => JavaList}

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator, Utilities}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.{PlanUtils, PartitionDesc}
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.rdd.RDD

import shark.api.TableRDD
import shark.{LogHelper, SharkEnv}


class SparkWork(
  val pctx: ParseContext,
  val terminalOperator: TerminalOperator,
  val resultSchema: JavaList[FieldSchema])
extends java.io.Serializable


/**
 * SparkTask executes a query plan composed of RDD operators.
 */
private[shark]
class SparkTask extends HiveTask[SparkWork] with Serializable with LogHelper {

  private var _tableRdd: Option[TableRDD] = None

  def tableRdd: Option[TableRDD] = _tableRdd

  override def execute(driverContext: DriverContext): Int = {
    logDebug("Executing " + this.getClass.getName)

    val ctx = driverContext.getCtx()

    // Adding files to the SparkContext
    // Added required files
    val files = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE)
    files.split(",").filterNot(x => x.isEmpty || SharkEnv.addedFiles.contains(x)).foreach { x =>
      logInfo("Adding file "  + x )
      SharkEnv.addedFiles.add(x)
      SharkEnv.sc.addFile(x)
    }

    // Added required jars
    val jars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR)
    jars.split(",").filterNot(x => x.isEmpty || SharkEnv.addedJars.contains(x)).foreach { x =>
      logInfo("Adding jar "  + x )
      SharkEnv.addedJars.add(x)
      SharkEnv.sc.addJar(x)
    }

    Operator.hconf = conf

    // Replace Hive physical plan with Shark plan.
    val terminalOp = work.terminalOperator
    val tableScanOps = terminalOp.returnTopOperators().asInstanceOf[Seq[TableScanOperator]]

    //ExplainTaskHelper.outputPlan(terminalOp, Console.out, true, 2)
    //ExplainTaskHelper.outputPlan(hiveTopOps.head, Console.out, true, 2)

    initializeTableScanTableDesc(tableScanOps)

    terminalOp.initializeMasterOnAll()

    // Set Spark's job description to be this query.
    SharkEnv.sc.setJobDescription(work.pctx.getContext.getCmd)

    // Set the fair scheduler's pool using mapred.fairscheduler.pool if it is defined.
    Option(conf.get("mapred.fairscheduler.pool")).foreach { pool =>
      SharkEnv.sc.setLocalProperty("spark.scheduler.pool", pool)
    }

    val sinkRdd = terminalOp.execute().asInstanceOf[RDD[Any]]

    val limit = terminalOp.parentOperators.head match {
      case op: LimitOperator => op.limit
      case _ => -1
    }

    if (terminalOp.isInstanceOf[TableRddSinkOperator]) {
      _tableRdd = Some(new TableRDD(sinkRdd, work.resultSchema, terminalOp.objectInspector, limit))
    }

    0
  }

  def initializeTableScanTableDesc(topOps: Seq[TableScanOperator]) {
    // topToTable maps Hive's TableScanOperator to the Table object.
    val topToTable: JHashMap[HiveTableScanOperator, Table] = work.pctx.getTopToTable()

    val emptyPartnArray = new Array[Partition](0)
    // Add table metadata to TableScanOperators
    topOps.foreach { op =>
      op.table = topToTable.get(op.hiveOp)
      op.tableDesc = Utilities.getTableDesc(op.table)
      PlanUtils.configureInputJobPropertiesForStorageHandler(op.tableDesc)
      if (op.table.isPartitioned) {
        val ppl = PartitionPruner.prune(
          op.table,
          work.pctx.getOpToPartPruner().get(op.hiveOp),
          work.pctx.getConf(), "",
          work.pctx.getPrunedPartitions())
        op.parts = ppl.getConfirmedPartns.toArray(emptyPartnArray) ++
          ppl.getUnknownPartns.toArray(emptyPartnArray)
        val allParts = op.parts ++ ppl.getDeniedPartns.toArray
        if (allParts.size == 0) {
          op.firstConfPartDesc = new PartitionDesc(op.tableDesc, null)
        } else {
          op.firstConfPartDesc = Utilities.getPartitionDesc(allParts(0).asInstanceOf[Partition])
        }
      }
    }
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}

