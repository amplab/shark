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

import java.util.ArrayList
import java.util.{List => JavaList}
import java.util.{Map => JavaMap}

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.metastore.api.{FieldSchema, MetaException}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.ql.exec.{DDLTask, FetchTask}
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.exec.MoveTask
import org.apache.hadoop.hive.ql.exec.{Operator => HiveOperator}
import org.apache.hadoop.hive.ql.exec.TaskFactory
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.session.SessionState

import shark.{LogHelper, SharkConfVars, SharkOptimizer}
import shark.execution.{HiveDesc, Operator, OperatorFactory, ReduceSinkOperator}
import shark.execution.{SharkDDLWork, SparkLoadWork, SparkWork, TerminalOperator}
import shark.memstore2.{CacheType, LazySimpleSerDeWrapper, MemoryMetadataManager}
import shark.memstore2.SharkTblProperties


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
   * Override SemanticAnalyzer.analyzeInternal to handle CTAS caching and INSERT updates.
   *
   * Unified views:
   *     For CTAS and INSERT INTO/OVERWRITE the generated Shark query plan matches the one
   * created if the target table were not cached. Disk => memory loading is done by a
   * SparkLoadTask that executes _after_ all other tasks (SparkTask, Hive MoveTasks) finish
   * executing. For INSERT INTO, the SparkLoadTask will be able to determine, using a path filter
   * based on a snapshot of the table/partition data directory taken in genMapRedTasks(), new files
   * that should be loaded into the cache. For CTAS, a path filter isn't used - everything in the
   * data directory is loaded into the cache.
   *
   * Non-unified views (i.e., the cached table content is memory-only):
   *     The query plan's FileSinkOperator is replaced by a MemoryStoreSinkOperator. The
   * MemoryStoreSinkOperator creates a new table (or partition) entry in the Shark metastore
   * for CTAS, and creates UnionRDDs for INSERT INTO commands.
   */
  override def analyzeInternal(ast: ASTNode): Unit = {
    reset()

    val qb = new QueryBlock(null, null, false)
    var pctx = getParseContext()
    pctx.setQB(qb)
    pctx.setParseTree(ast)
    initParseCtx(pctx)
    // The ASTNode that will be analyzed by SemanticAnalzyer#doPhase1().
    var child: ASTNode = ast

    logDebug("Starting Shark Semantic Analysis")

    //TODO: can probably reuse Hive code for this
    var shouldReset = false

    val astTokenType = ast.getToken().getType()
    if (astTokenType == HiveParser.TOK_CREATEVIEW || astTokenType == HiveParser.TOK_ANALYZE) {
      // Delegate create view and analyze to Hive.
      super.analyzeInternal(ast)
      return
    } else if (astTokenType == HiveParser.TOK_CREATETABLE) {
      init()
      // Use Hive to do a first analysis pass.
      super.analyzeInternal(ast)
      // Do post-Hive analysis of the CREATE TABLE (e.g detect caching mode).
      analyzeCreateTable(ast, qb) match {
        case Some(queryStmtASTNode) => {
          // Set the 'child' to reference the SELECT statement root node, with is a
          // HiveParer.HIVE_QUERY.
          child = queryStmtASTNode
          // Hive's super.analyzeInternal() might generate MapReduce tasks. Avoid executing those
          // tasks by reset()-ing some Hive SemanticAnalyzer state after doPhase1() is called below.
          shouldReset = true
        }
        case None => {
          // Done with semantic analysis if the CREATE TABLE statement isn't a CTAS.
          return
        }
      }
    } else {
      SessionState.get().setCommandType(HiveOperation.QUERY)
    }

    // Invariant: At this point, the command will execute a query (i.e., its AST contains a
    //     HiveParser.TOK_QUERY node).

    // Continue analyzing from the child ASTNode.
    if (!doPhase1(child, qb, initPhase1Ctx())) {
      return
    }

    // Used to protect against recursive views in getMetaData().
    SharkSemanticAnalyzer.viewsExpandedField.set(this, new ArrayList[String]())

    logDebug("Completed phase 1 of Shark Semantic Analysis")
    getMetaData(qb)
    logDebug("Completed getting MetaData in Shark Semantic Analysis")

    // Reset makes sure we don't run the mapred jobs generated by Hive.
    if (shouldReset) {
      reset()
    }

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
    val optm = new SharkOptimizer()
    optm.setPctx(pctx)
    optm.initialize(conf)
    pctx = optm.optimize()

    // Replace Hive physical plan with Shark plan. This needs to happen after
    // Hive optimization.
    val hiveSinkOps = SharkSemanticAnalyzer.findAllHiveFileSinkOperators(
      pctx.getTopOps().values().head)

    // TODO: clean the following code. It's too messy to understand...
    val terminalOpSeq = {
      val qbParseInfo = qb.getParseInfo
      if (qbParseInfo.isInsertToTable && !qb.isCTAS) {
        // Handle INSERT. There can be multiple Hive sink operators if the single command comprises
        // multiple INSERTs.
        hiveSinkOps.map { hiveSinkOp =>
          val tableDesc = hiveSinkOp.asInstanceOf[HiveFileSinkOperator].getConf().getTableInfo()
          val tableName = tableDesc.getTableName
          if (tableName == null || tableName == "") {
            // If table name is empty, it is an INSERT (OVERWRITE) DIRECTORY.
            OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
          } else {
            // Otherwise, check if we are inserting into a table that was cached.
            val tableNameSplit = tableName.split('.') // Split from 'databaseName.tableName'
            val cachedTableName = tableNameSplit(1)
            val databaseName = tableNameSplit(0)
            val hiveTable = Hive.get().getTable(databaseName, tableName)
            val cacheMode = CacheType.fromString(
              hiveTable.getProperty(SharkTblProperties.CACHE_FLAG.varname))
            if (CacheType.shouldCache(cacheMode)) {
              if (hiveSinkOps.size == 1) {
                // INSERT INTO or OVERWRITE update on a cached table.
                qb.targetTableDesc = tableDesc
                // If isInsertInto is true, the sink op is for INSERT INTO.
                val isInsertInto = qbParseInfo.isInsertIntoTable(databaseName, cachedTableName)
                val isPartitioned = hiveTable.isPartitioned
                var hivePartitionKeyOpt = if (isPartitioned) {
                  Some(SharkSemanticAnalyzer.getHivePartitionKey(qb))
                } else {
                  None
                }
                if (cacheMode == CacheType.MEMORY) {
                  // The table being updated is stored in memory and backed by disk, a
                  // SparkLoadTask will be created by the genMapRedTasks() call below. Set fields
                  // in `qb` that will be needed.
                  qb.cacheMode = cacheMode
                  qb.targetTableDesc = tableDesc
                  OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
                } else {
                  OperatorFactory.createSharkMemoryStoreOutputPlan(
                    hiveSinkOp,
                    cachedTableName,
                    databaseName,
                    _resSchema.size,  /* numColumns */
                    hivePartitionKeyOpt,
                    cacheMode,
                    isInsertInto)
                }
              } else {
                throw new SemanticException(
                  "Shark does not support updating cached table(s) with multiple INSERTs")
              }
            } else {
              OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
            }
          }
        }
      } else if (hiveSinkOps.size == 1) {
        Seq {
          // For a single output, we have the option of choosing the output
          // destination (e.g. CTAS with table property "shark.cache" = "true").
          if (qb.isCTAS && qb.createTableDesc != null && CacheType.shouldCache(qb.cacheMode)) {
            // The table being created from CTAS should be cached.
            val tblProps = qb.createTableDesc.getTblProps
            if (qb.cacheMode == CacheType.MEMORY) {
              // Save the preferred storage level, since it's needed to create a SparkLoadTask in
              // genMapRedTasks().
              OperatorFactory.createSharkFileOutputPlan(hiveSinkOps.head)
            } else {
              OperatorFactory.createSharkMemoryStoreOutputPlan(
                hiveSinkOps.head,
                qb.createTableDesc.getTableName,
                qb.createTableDesc.getDatabaseName,
                numColumns = _resSchema.size,
                hivePartitionKeyOpt = None,
                qb.cacheMode,
                isInsertInto = false)
            }
          } else if (pctx.getContext().asInstanceOf[QueryContext].useTableRddSink && !qb.isCTAS) {
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

    logDebug("Completed plan generation")
  }

  /**
   * Generate tasks for executing the query, including the SparkTask to do the
   * select, the MoveTask for updates, and the DDLTask for CTAS.
   */
  def genMapRedTasks(qb: QueryBlock, pctx: ParseContext, terminalOps: Seq[TerminalOperator]) {
    // Create the spark task.
    terminalOps.foreach { terminalOp =>
      val task = TaskFactory.get(new SparkWork(pctx, terminalOp, _resSchema), conf)
      rootTasks.add(task)
    }

    if (qb.getIsQuery) {
      // Note: CTAS isn't considered a query - it's handled in the 'else' block below.
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
      // Configure MoveTasks for CTAS, INSERT.
      val mvTasks = new ArrayList[MoveTask]()

      // For CTAS, `fileWork` contains a single LoadFileDesc (called "LoadFileWork" in Hive).
      val fileWork = getParseContext.getLoadFileWork
      val tableWork = getParseContext.getLoadTableWork
      tableWork.foreach { ltd =>
        mvTasks.add(TaskFactory.get(
          new MoveWork(null, null, ltd, null, false), conf).asInstanceOf[MoveTask])
      }

      fileWork.foreach { lfd =>
        if (qb.isCTAS) {
          // For CTAS, `lfd.targetDir` references the data directory of the table being created.
          var location = qb.getTableDesc.getLocation
          if (location == null) {
            try {
              val tableToCreate = db.newTable(qb.getTableDesc.getTableName)
              val wh = new Warehouse(conf)
              location = wh.getTablePath(db.getDatabase(tableToCreate.getDbName()), tableToCreate
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

      // The move task depends on all root tasks. In the case of multiple outputs,
      // the moves are only started once all outputs are executed.
      // Note: For a CTAS for a memory-only cached table, a MoveTask is still added as a child of
      // the main SparkTask. However, there no effects from its execution, since the SELECT query
      // output is piped to Shark's in-memory columnar storage builder, instead of a Hive tmp
      // directory.
      // TODO(harvey): Don't create a MoveTask in this case.
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

      if (qb.cacheMode == CacheType.MEMORY) {
        // Create a SparkLoadTask used to scan and load disk contents into the cache.
        val sparkLoadWork = if (qb.isCTAS) {
          // For cached tables, Shark-specific table properties should be set in
          // analyzeCreateTable().
          val tblProps = qb.createTableDesc.getTblProps

          // No need to create a filter, since the entire table data directory should be loaded, nor
          // pass partition specifications, since partitioned tables can't be created from CTAS.
          val sparkLoadWork = new SparkLoadWork(
            qb.createTableDesc.getDatabaseName,
            qb.createTableDesc.getTableName,
            SparkLoadWork.CommandTypes.NEW_ENTRY,
            qb.cacheMode)
          sparkLoadWork
        } else {
          // Split from 'databaseName.tableName'
          val tableNameSplit = qb.targetTableDesc.getTableName.split('.')
          val databaseName = tableNameSplit(0)
          val cachedTableName = tableNameSplit(1)
          val hiveTable = db.getTable(databaseName, cachedTableName)
          // None if the table isn't partitioned, or if the partition specified doesn't exist.
          val partSpecOpt = Option(qb.getMetaData.getDestPartitionForAlias(
            qb.getParseInfo.getClauseNamesForDest.head)).map(_.getSpec)
          SparkLoadWork(
            db,
            conf,
            hiveTable,
            partSpecOpt,
            isOverwrite = !qb.getParseInfo.isInsertIntoTable(databaseName, cachedTableName))
        }
        // Add a SparkLoadTask as a dependent of all MoveTasks, so that when executed, the table's
        // (or table partition's) data directory will already contain updates that should be
        // loaded into memory.
        val sparkLoadTask = TaskFactory.get(sparkLoadWork, conf)
        mvTasks.foreach(_.addDependentTask(sparkLoadTask))
      }
    }

    // For CTAS, generate a DDL task to create the table. This task should be a
    // dependent of the main SparkTask.
    if (qb.isCTAS) {
      val crtTblDesc: CreateTableDesc = qb.getTableDesc
      crtTblDesc.validate()

      // Clear the output for CTAS since we don't need the output from the
      // mapredWork, the DDLWork at the tail of the chain will have the output.
      getOutputs.clear()

      // CTAS assumes only single output.
      val crtTblTask = TaskFactory.get(
        new DDLWork(getInputs, getOutputs, crtTblDesc),conf).asInstanceOf[DDLTask]
      rootTasks.head.addDependentTask(crtTblTask)
    }
  }

  def analyzeCreateTable(rootAST: ASTNode, queryBlock: QueryBlock): Option[ASTNode] = {
    // If we detect that the CREATE TABLE is part of a CTAS, then this is set to the root node of
    // the query command (i.e., the root node of the SELECT statement).
    var queryStmtASTNode: Option[ASTNode] = None

    // TODO(harvey): We might be able to reuse the QB passed into this method, as long as it was
    //               created after the super.analyzeInternal() call. That QB and the createTableDesc
    //               should have everything (e.g. isCTAS(), partCols). Note that the QB might not be
    //               accessible from getParseContext(), since the SemanticAnalyzer#analyzeInternal()
    //               doesn't set (this.qb = qb) for a non-CTAS.
    // True if the command is a CREATE TABLE, but not a CTAS.
    var isRegularCreateTable = true
    var isHivePartitioned = false

    for (ch <- rootAST.getChildren) {
      ch.asInstanceOf[ASTNode].getToken.getType match {
        case HiveParser.TOK_QUERY => {
        isRegularCreateTable = false
          queryStmtASTNode = Some(ch.asInstanceOf[ASTNode])
        }
        case _ => Unit
      }
    }

    var ddlTasks: Seq[DDLTask] = Nil
    val createTableDesc = if (isRegularCreateTable) {
      // Unfortunately, we have to comb the root tasks because for CREATE TABLE,
      // SemanticAnalyzer#analyzeCreateTable() does't set the CreateTableDesc in its QB.
      ddlTasks = rootTasks.filter(_.isInstanceOf[DDLTask]).asInstanceOf[Seq[DDLTask]]
      if (ddlTasks.isEmpty) null else ddlTasks.head.getWork.getCreateTblDesc
    } else {
      getParseContext.getQB.getTableDesc
    }

    // Update the QueryBlock passed into this method.
    // TODO(harvey): Remove once the TODO above is fixed.
    queryBlock.setTableDesc(createTableDesc)

    // 'createTableDesc' is NULL if there is an IF NOT EXISTS condition and the target table
    // already exists.
    if (createTableDesc != null) {
      val tableName = createTableDesc.getTableName
      val checkTableName = SharkConfVars.getBoolVar(conf, SharkConfVars.CHECK_TABLENAME_FLAG)
      // Note that the CreateTableDesc's table properties are Java Maps, but the TableDesc's table
      // properties, which are used during execution, are Java Properties.
      val createTableProperties: JavaMap[String, String] = createTableDesc.getTblProps()

      // There are two cases that will enable caching:
      // 1) Table name includes "_cached" or "_offheap".
      // 2) The "shark.cache" table property is "true", or the string representation of a supported
      //    cache mode (memory, memory-only, Tachyon).
      var cacheMode = CacheType.fromString(
        createTableProperties.get(SharkTblProperties.CACHE_FLAG.varname))
      if (checkTableName) {
        // Use memory only mode for _cached tables, unless the mode is already specified.
        if (tableName.endsWith("_cached") && cacheMode == CacheType.NONE) {
          cacheMode = CacheType.MEMORY_ONLY
        } else if (tableName.endsWith("_tachyon")) {
          logWarning("'*_tachyon' names are deprecated, please cache using '*_offheap'")
          cacheMode = CacheType.OFFHEAP
        } else if (tableName.endsWith("_offheap")) {
          cacheMode = CacheType.OFFHEAP
        }
      }

      // Continue planning based on the 'cacheMode' read.
      val shouldCache = CacheType.shouldCache(cacheMode)
      if (shouldCache) {
        if (cacheMode == CacheType.MEMORY_ONLY || cacheMode == CacheType.OFFHEAP) {
          val serDeName = createTableDesc.getSerName
          if (serDeName == null || serDeName == classOf[LazySimpleSerDe].getName) {
            // Hive's SemanticAnalyzer optimizes based on checks for LazySimpleSerDe, which causes
            // casting exceptions for cached table scans during runtime. Use a simple SerDe wrapper
            // to guard against these optimizations.
            createTableDesc.setSerName(classOf[LazySimpleSerDeWrapper].getName)
          }
        }
        createTableProperties.put(SharkTblProperties.CACHE_FLAG.varname, cacheMode.toString)
      }

      // For CTAS ('isRegularCreateTable' is false), the MemoryStoreSinkOperator creates a new
      // table metadata entry in the MemoryMetadataManager. The SparkTask that encloses the
      // MemoryStoreSinkOperator will have a child Hive DDLTask, which creates a new table metadata
      // entry in the Hive metastore. See genMapRedTasks() for SparkTask creation.
      if (isRegularCreateTable && shouldCache) {
        // In Hive, a CREATE TABLE command is handled by a DDLTask, created by
        // SemanticAnalyzer#analyzeCreateTable(), in 'rootTasks'. The DDL tasks' execution succeeds
        // only if the CREATE TABLE is valid. So, hook a SharkDDLTask as a child of the Hive DDLTask
        // so that Shark metadata is updated only if the Hive task execution is successful.
        val hiveDDLTask = ddlTasks.head
        val sharkDDLWork = new SharkDDLWork(createTableDesc)
        sharkDDLWork.cacheMode = cacheMode
        hiveDDLTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
      }

      queryBlock.cacheMode = cacheMode
      queryBlock.setTableDesc(createTableDesc)
    }
    queryStmtASTNode
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

  private def getHivePartitionKey(qb: QB): String = {
    val selectClauseKey = qb.getParseInfo.getClauseNamesForDest.head
    val destPartition = qb.getMetaData.getDestPartitionForAlias(selectClauseKey)
    val partitionColumns = destPartition.getTable.getPartCols.map(_.getName)
    val partitionColumnToValue = destPartition.getSpec
    MemoryMetadataManager.makeHivePartitionKeyStr(partitionColumns, partitionColumnToValue)
  }
  
  /**
   * Given a Hive top operator (e.g. TableScanOperator), find all the file sink
   * operators (aka file output operator).
   */
  private def findAllHiveFileSinkOperators(op: HiveOperator[_<: HiveDesc])
  : Seq[HiveOperator[_<: HiveDesc]] = {
    if (op.getChildOperators() == null || op.getChildOperators().size() == 0) {
      Seq[HiveOperator[_<: HiveDesc]](op)
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
    val queue = new scala.collection.mutable.Queue[Operator[_ <: HiveDesc]]
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
  }
}
