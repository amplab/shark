package shark.parse

import java.io.IOException
import java.util.{List => JavaList}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.{Context, ErrorMsg}
import org.apache.hadoop.hive.ql.parse.{ASTNode, ColumnStatsSemanticAnalyzer, ParseDriver, 
  ParseException, SemanticException}
import shark.{LogHelper}

class SharkColumnStatsSemanticAnalyzer(conf: HiveConf, tree: ASTNode) 
  extends ColumnStatsSemanticAnalyzer(conf, tree) with LogHelper {

  var sem: SharkSemanticAnalyzer = _ 
  def getSharkSem: SharkSemanticAnalyzer = sem

  /**
   * Hive's ColumnStats SA rewrites the query and tree, then calls Hive's SemanticAnalyzer. 
   * We need to override this to call SharkSemanticAnalyzer instead to get Shark operators. 
   */
  override def analyze(ast: ASTNode, origCtx: Context): Unit = {
    // Get private parent variables using reflection
    val _colNamesF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("colNames")
    val _colTypeF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("colType")
    val _ctxF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("ctx")
    val _isRewrittenF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("isRewritten")
    val _isTableLevelF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("isTableLevel")
    val _originalTreeF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("originalTree")
    val _partNameF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("partName")
    val _rewrittenTreeF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("rewrittenTree")
    val _tableNameF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("tableName")

    // Make private variables accessible 
    _colNamesF.setAccessible(true)
    _colTypeF.setAccessible(true)
    _ctxF.setAccessible(true)
    _isRewrittenF.setAccessible(true)
    _isTableLevelF.setAccessible(true)
    _originalTreeF.setAccessible(true)
    _partNameF.setAccessible(true)
    _rewrittenTreeF.setAccessible(true)
    _tableNameF.setAccessible(true)

    // Initialize QB
    init()

    // Set up the necessary metadata if originating from analyze rewrite
    if (_isRewrittenF.get(this).asInstanceOf[Boolean]) {
      val rewrittenCtx = rewriteContext
      val qb = getQB()
      qb.setAnalyzeRewrite(true)
      val qbp = qb.getParseInfo()
      qbp.setTableName(_tableNameF.get(this).asInstanceOf[String])
      qbp.setTblLvl(_isTableLevelF.get(this).asInstanceOf[Boolean])

      if (!_isTableLevelF.get(this).asInstanceOf[Boolean]) {
        qbp.setPartName(_partNameF.get(this).asInstanceOf[String])
      }
      qbp.setColName(_colNamesF.get(this).asInstanceOf[JavaList[String]])
      qbp.setColType(_colTypeF.get(this).asInstanceOf[JavaList[String]])
      initCtx(rewrittenCtx)
      logInfo("Invoking analyze on rewritten query")

      // Create a Shark semantic analyzer for rewritten query 
      val tree = _rewrittenTreeF.get(this).asInstanceOf[ASTNode]
      sem = new SharkSemanticAnalyzer(conf)
      sem.init()
      sem.initCtx(rewrittenCtx)
      sem.setQB(qb)
      sem.analyzeInternal(tree)

    } else {
      initCtx(origCtx)
      init()
      logInfo("Invoking analyze on original query")

      // Create a Shark semantic analyzer for original query
      val tree = _originalTreeF.get(this).asInstanceOf[ASTNode]
      sem = new SharkSemanticAnalyzer(conf)
      sem.init()
      sem.initCtx(origCtx)
      sem.analyzeInternal(tree)
    }
  }

  /**
   * If query was rewritten, we need to generate a new QueryContext, because Hive 
   * generated a new Context instance.
   */
  def rewriteContext: QueryContext = {
      // Use reflection to get the rewritten query
      val _rewrittenQueryF = classOf[ColumnStatsSemanticAnalyzer].getDeclaredField("rewrittenQuery")
      _rewrittenQueryF.setAccessible(true)
      val rewrittenQuery = _rewrittenQueryF.get(this).asInstanceOf[String]
      var ctx: QueryContext = null

      try {
        // Initialize with useTableRddSink=false
        ctx = new QueryContext(conf, false)
      } catch {
        case e: IOException => 
          throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_IO_ERROR.getMsg())
      }
      ctx.setCmd(rewrittenQuery)
      ctx
  }
}
