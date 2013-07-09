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

package shark.parse

import java.lang.reflect.Method
import java.util.ArrayList
import java.util.{List => JavaList}

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, MetaException}
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.ql.exec.{DDLTask, FetchTask, MoveTask, TaskFactory}
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.optimizer.Optimizer
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.session.SessionState

import shark.{CachedTableRecovery, LogHelper, SharkConfVars, SharkEnv, RDDUtils,  Utils}
import shark.execution.{HiveOperator, Operator, OperatorFactory, ReduceSinkOperator, SparkWork,
  TerminalOperator}
import shark.memstore2.{CacheType, ColumnarSerDe, MemoryMetadataManager}

import spark.storage.StorageLevel


/**
 * Shark's version of Hive's SemanticAnalyzer. In SemanticAnalyzer,
 * genMapRedTasks() breaks the query plan down to different stages because of
 * mapreduce. We want our query plan to stay intact as a single tree. Since
 * genMapRedTasks is private, we have to overload analyzeInternal() to use our
 * own genMapRedTasks().
 */
class SharkSemanticAnalyzer(conf: HiveConf) extends SemanticAnalyzer(conf) with LogHelper {

  var _resSchema: JavaList[FieldSchema] = null

  /**
   * This is used in driver to get the result schema.
   */
  override def getResultSchema() = _resSchema

  /**
   * Override SemanticAnalyzer.analyzeInternal to handle CTAS caching.
   */
  override def analyzeInternal(ast: ASTNode): Unit = {
    reset()

    val qb = new QB(null, null, false)
    val pctx = getParseContext()
    pctx.setQB(qb)
    pctx.setParseTree(ast)
    init(pctx)
    var child: ASTNode = ast

    logInfo("Starting Shark Semantic Analysis")

    //TODO: can probably reuse Hive code for this
    // analyze create table command
    var cacheMode = CacheType.none
    var isCTAS = false
    var shouldReset = false

    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      super.analyzeInternal(ast)
      for (ch <- ast.getChildren) {
        ch.asInstanceOf[ASTNode].getToken.getType match {
          case HiveParser.TOK_QUERY => {
            isCTAS = true
            child = ch.asInstanceOf[ASTNode]
          }
          case _ =>
            Unit
        }
      }

      // If the table descriptor can be null if the CTAS has an
      // "if not exists" condition.
      val td = getParseContext.getQB.getTableDesc
      if (!isCTAS || td == null) {
        return
      } else {
        val checkTableName = SharkConfVars.getBoolVar(conf, SharkConfVars.CHECK_TABLENAME_FLAG)
        val cacheType = CacheType.fromString(td.getTblProps().get("shark.cache"))
        if (cacheType == CacheType.heap ||
          (td.getTableName.endsWith("_cached") && checkTableName)) {
          cacheMode = CacheType.heap
          td.getTblProps().put("shark.cache", cacheMode.toString)
        } else if (cacheType == CacheType.tachyon ||
          (td.getTableName.endsWith("_tachyon") && checkTableName)) {
          cacheMode = CacheType.tachyon
          td.getTblProps().put("shark.cache", cacheMode.toString)
        }

        if (CacheType.shouldCache(cacheMode)) {
          td.setSerName(classOf[ColumnarSerDe].getName)
        }

        qb.setTableDesc(td)
        shouldReset = true
      }
    } else {
      SessionState.get().setCommandType(HiveOperation.QUERY)
    }

    // Delegate create view and analyze to Hive.
    val astTokenType = ast.getToken().getType()
    if (astTokenType == HiveParser.TOK_CREATEVIEW || astTokenType == HiveParser.TOK_ANALYZE) {
      return super.analyzeInternal(ast)
    }

    // Continue analyzing from the child ASTNode.
    if (!doPhase1(child, qb, initPhase1Ctx())) {
      return
    }

    // Used to protect against recursive views in getMetaData().
    SharkSemanticAnalyzer.viewsExpandedField.set(this, new ArrayList[String]())

    logInfo("Completed phase 1 of Shark Semantic Analysis")
    getMetaData(qb)
    logInfo("Completed getting MetaData in Shark Semantic Analysis")

    // Reset makes sure we don't run the mapred jobs generated by Hive.
    if (shouldReset) reset()

    // Save the result schema derived from the sink operator produced
    // by genPlan. This has the correct column names, which clients
    // such as JDBC would prefer instead of the c0, c1 we'll end
    // up with later.
    val hiveSinkOp = genPlan(qb).asInstanceOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator]

    // Use reflection to invoke convertRowSchemaToViewSchema.
    _resSchema = SharkSemanticAnalyzer.convertRowSchemaToViewSchemaMethod.invoke(
      this, pctx.getOpParseCtx.get(hiveSinkOp).getRowResolver()
      ).asInstanceOf[JavaList[FieldSchema]]

    // Run Hive optimization.
    var pCtx: ParseContext = getParseContext
    val optm = new Optimizer()
    optm.setPctx(pCtx)
    optm.initialize(conf)
    pCtx = optm.optimize()
    init(pCtx)

    // Replace Hive physical plan with Shark plan. This needs to happen after
    // Hive optimization.
    val hiveSinkOps = SharkSemanticAnalyzer.findAllHiveFileSinkOperators(
      pCtx.getTopOps().values().head)

    // TODO: clean the following code. It's too messy to understand...
    val terminalOpSeq = {
      if (qb.getParseInfo.isInsertToTable && !qb.isCTAS) {
        hiveSinkOps.map { hiveSinkOp =>
          val tableName = hiveSinkOp.asInstanceOf[HiveFileSinkOperator].getConf().getTableInfo()
            .getTableName()

          if (tableName == null || tableName == "") {
            // If table name is empty, it is an INSERT (OVERWRITE) DIRECTORY.
            OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
          } else {
            // Otherwise, check if we are inserting into a table that was cached.
            val cachedTableName = tableName.split('.')(1) // Ignore the database name
            SharkEnv.memoryMetadataManager.get(cachedTableName) match {
              case Some(rdd) => {
                if (hiveSinkOps.size == 1) {
                  // If useUnionRDD is false, the sink op is for INSERT OVERWRITE.
                  val useUnionRDD = qb.getParseInfo.isInsertIntoTable(cachedTableName)
                  val storageLevel = RDDUtils.getStorageLevelOfCachedTable(rdd)
                  OperatorFactory.createSharkMemoryStoreOutputPlan(
                    hiveSinkOp,
                    cachedTableName,
                    storageLevel,
                    _resSchema.size,                // numColumns
                    cacheMode == CacheType.tachyon, // use tachyon
                    useUnionRDD)
                } else {
                  throw new SemanticException(
                    "Shark does not support updating cached table(s) with multiple INSERTs")
                }
              }
              case None => OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
            }
          }
        }
      } else if (hiveSinkOps.size == 1) {
        // For a single output, we have the option of choosing the output
        // destination (e.g. CTAS with table property "shark.cache" = "true").
        Seq {
          if (qb.isCTAS && qb.getTableDesc != null && CacheType.shouldCache(cacheMode)) {
            val storageLevel = MemoryMetadataManager.getStorageLevelFromString(
              qb.getTableDesc().getTblProps.get("shark.cache.storageLevel"))
            qb.getTableDesc().getTblProps().put(CachedTableRecovery.QUERY_STRING, ctx.getCmd())
            OperatorFactory.createSharkMemoryStoreOutputPlan(
              hiveSinkOps.head,
              qb.getTableDesc.getTableName,
              storageLevel,
              _resSchema.size,                // numColumns
              cacheMode == CacheType.tachyon, // use tachyon
              false)
          } else if (pctx.getContext().asInstanceOf[QueryContext].useTableRddSink) {
            OperatorFactory.createSharkRddOutputPlan(hiveSinkOps.head)
          } else {
            OperatorFactory.createSharkFileOutputPlan(hiveSinkOps.head)
          }
        }

        // A hack for the query plan dashboard to get the query plan. This was
        // done for SIGMOD demo. Turn it off by default.
        //shark.dashboard.QueryPlanDashboardHandler.terminalOperator = terminalOp

      } else {
        // For non-INSERT commands, if there are multiple file outputs, we always use file outputs.
        hiveSinkOps.map(OperatorFactory.createSharkFileOutputPlan(_))
      }
    }

    SharkSemanticAnalyzer.breakHivePlanByStages(terminalOpSeq)
    genMapRedTasks(qb, pctx, terminalOpSeq)

    logInfo("Completed plan generation")
  }

  /**
   * Generate tasks for executing the query, including the SparkTask to do the
   * select, the MoveTask for updates, and the DDLTask for CTAS.
   */
  def genMapRedTasks(qb: QB, pctx: ParseContext, terminalOps: Seq[TerminalOperator]) {

    // Create the spark task.
    terminalOps.foreach { terminalOp =>
      val task = TaskFactory.get(new SparkWork(pctx, terminalOp, _resSchema), conf)
      rootTasks.add(task)
    }

    if (qb.getIsQuery) {
      // Configure FetchTask (used for fetching results to CLIDriver).
      val loadWork = getParseContext.getLoadFileWork.get(0)
      val cols = loadWork.getColumns
      val colTypes = loadWork.getColumnTypes

      val resFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT)
      val resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat)

      val fetchWork = new FetchWork(
        new Path(loadWork.getSourceDir).toString, resultTab, qb.getParseInfo.getOuterQueryLimit)

      val fetchTask = TaskFactory.get(fetchWork, conf).asInstanceOf[FetchTask]
      setFetchTask(fetchTask)

    } else {
      // Configure MoveTasks for table updates (e.g. CTAS, INSERT).
      val mvTasks = new ArrayList[MoveTask]()

      val fileWork = getParseContext.getLoadFileWork
      val tableWork = getParseContext.getLoadTableWork
      tableWork.foreach { ltd =>
        mvTasks.add(TaskFactory.get(
          new MoveWork(null, null, ltd, null, false), conf).asInstanceOf[MoveTask])
      }

      fileWork.foreach { lfd =>
        if (qb.isCTAS) {
          var location = qb.getTableDesc.getLocation
          if (location == null) {
            try {
              val dumpTable = db.newTable(qb.getTableDesc.getTableName)
              val wh = new Warehouse(conf)
              location = wh.getTablePath(db.getDatabase(dumpTable.getDbName()), dumpTable
                  .getTableName()).toString;
            } catch {
              case e: HiveException => throw new SemanticException(e)
              case e: MetaException => throw new SemanticException(e)
            }
          }
          lfd.setTargetDir(location)
        }

        mvTasks.add(TaskFactory.get(
          new MoveWork(null, null, null, lfd, false), conf).asInstanceOf[MoveTask])
      }

      // The move task depends on all root tasks. In the case of multi outputs,
      // the moves are only started once all outputs are executed.
      val hiveFileSinkOp = terminalOps.head.hiveOp
      mvTasks.foreach { moveTask =>
        rootTasks.foreach { rootTask =>
          rootTask.addDependentTask(moveTask)
        }

        // Add StatsTask's. See GenMRFileSink1.addStatsTask().
        /*
        if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          println("Adding a StatsTask for MoveTask " + moveTask)
          //addStatsTask(fsOp, mvTask, currTask, parseCtx.getConf())
          val statsWork = new StatsWork(moveTask.getWork().getLoadTableWork())
          statsWork.setAggKey(hiveFileSinkOp.getConf().getStatsAggPrefix())
          val statsTask = TaskFactory.get(statsWork, conf)
          hiveFileSinkOp.getConf().setGatherStats(true)
          moveTask.addDependentTask(statsTask)
          statsTask.subscribeFeed(moveTask)
        }
        */
      }
    }

    // For CTAS, generate a DDL task to create the table. This task should be a
    // dependent of the main SparkTask.
    if (qb.isCTAS) {
      val crtTblDesc: CreateTableDesc = qb.getTableDesc

      // Use reflection to call validateCreateTable, which is private.
      val validateCreateTableMethod = this.getClass.getSuperclass.getDeclaredMethod(
        "validateCreateTable", classOf[CreateTableDesc])
      validateCreateTableMethod.setAccessible(true)
      validateCreateTableMethod.invoke(this, crtTblDesc)

      // Clear the output for CTAS since we don't need the output from the
      // mapredWork, the DDLWork at the tail of the chain will have the output.
      getOutputs.clear()

      // CTAS assumes only single output.
      val crtTblTask = TaskFactory.get(
        new DDLWork(getInputs, getOutputs, crtTblDesc),conf).asInstanceOf[DDLTask]
      rootTasks.head.addDependentTask(crtTblTask)
    }
  }
}


object SharkSemanticAnalyzer extends LogHelper {

  /**
   * The reflection object used to invoke convertRowSchemaToViewSchema.
   */
  private val convertRowSchemaToViewSchemaMethod = classOf[SemanticAnalyzer].getDeclaredMethod(
    "convertRowSchemaToViewSchema", classOf[RowResolver])
  convertRowSchemaToViewSchemaMethod.setAccessible(true)

  /**
   * The reflection object used to get a reference to SemanticAnalyzer.viewsExpanded,
   * so we can initialize it.
   */
  private val viewsExpandedField = classOf[SemanticAnalyzer].getDeclaredField("viewsExpanded")
  viewsExpandedField.setAccessible(true)

  /**
   * Given a Hive top operator (e.g. TableScanOperator), find all the file sink
   * operators (aka file output operator).
   */
  private def findAllHiveFileSinkOperators(op: HiveOperator): Seq[HiveOperator] = {
    if (op.getChildOperators() == null || op.getChildOperators().size() == 0) {
      Seq[HiveOperator](op)
    } else {
      op.getChildOperators().flatMap(findAllHiveFileSinkOperators(_)).distinct
    }
  }

  /**
   * Break the Hive operator tree into multiple stages, separated by Hive
   * ReduceSink. This is necessary because the Hive operators after ReduceSink
   * cannot be initialized using ReduceSink's output object inspector. We
   * craft the struct object inspector (that has both KEY and VALUE) in Shark
   * ReduceSinkOperator.initializeDownStreamHiveOperators().
   */
  private def breakHivePlanByStages(terminalOps: Seq[TerminalOperator]) = {
    val reduceSinks = new scala.collection.mutable.HashSet[ReduceSinkOperator]
    val queue = new scala.collection.mutable.Queue[Operator[_]]
    queue ++= terminalOps

    while (!queue.isEmpty) {
      val current = queue.dequeue()
      current match {
        case op: ReduceSinkOperator => reduceSinks += op
        case _ => Unit
      }
      // This is not optimal because operators can be added twice. But the
      // operator tree should not be too big...
      queue ++= current.parentOperators
    }

    logDebug("Found %d ReduceSinkOperator's.".format(reduceSinks.size))

    reduceSinks.foreach { op =>
      val hiveOp = op.asInstanceOf[Operator[HiveOperator]].hiveOp
      if (hiveOp.getChildOperators() != null) {
        hiveOp.getChildOperators().foreach { child =>
          logDebug("Removing child %s from %s".format(child, hiveOp))
          hiveOp.removeChild(child)
        }
      }
    }
  }
}
