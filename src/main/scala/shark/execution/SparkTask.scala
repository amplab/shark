package shark.execution

import java.util.{HashMap => JHashMap, List => JavaList}
import java.io.File

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, PartitionDesc}
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.ql.session.SessionState

import scala.collection.JavaConversions._

import shark.{LogHelper, SharkEnv}
import spark.RDD


class SparkWork(
  val pctx: ParseContext,
  val terminalOperator: TerminalOperator,
  val resultSchema: JavaList[FieldSchema])
extends java.io.Serializable


/**
 * SparkTask executes a query plan composed of RDD operators.
 */
class SparkTask extends org.apache.hadoop.hive.ql.exec.Task[SparkWork]
with java.io.Serializable with LogHelper {

  private var _tableRdd: TableRDD = null
  def tableRdd = _tableRdd

  override def execute(driverContext: DriverContext): Int = {
    logInfo("Executing " + this.getClass.getName)
    
    val ctx = driverContext.getCtx()

    // Adding files to the SparkContext
    // Added required files
    val files = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE)
    files.split(",").filterNot(_.isEmpty).foreach { SharkEnv.sc.addFile(_) }

    // Added required jars
    val jars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR)
    jars.split(",").filterNot(_.isEmpty).foreach { SharkEnv.sc.addJar(_) }

    Operator.hconf = conf

    // Replace Hive physical plan with Shark plan.
    val terminalOp = work.terminalOperator
    val tableScanOps = terminalOp.returnTopOperators().asInstanceOf[Seq[TableScanOperator]]

    //ExplainTaskHelper.outputPlan(terminalOp, Console.out, true, 2)
    //ExplainTaskHelper.outputPlan(hiveTopOps.head, Console.out, true, 2)

    initializeTableScanTableDesc(tableScanOps)

    // Initialize the Hive query plan. This gives us all the object inspectors.
    initializeAllHiveOperators(terminalOp)

    terminalOp.initializeMasterOnAll()

    val sinkRdd = terminalOp.execute().asInstanceOf[RDD[Any]]

    _tableRdd = new TableRDD(sinkRdd, work.resultSchema, terminalOp.objectInspector)
    0
  }

  def initializeTableScanTableDesc(topOps: Seq[TableScanOperator]) {
    // topToTable maps Hive's TableScanOperator to the Table object.
    val topToTable: JHashMap[HiveTableScanOperator, Table] = work.pctx.getTopToTable()

    // Add table metadata to TableScanOperators
    topOps.foreach { op =>
      op.table = topToTable.get(op.hiveOp)
      op.tableDesc = Utilities.getTableDesc(op.table)
      if (op.table.isPartitioned) {
        val ppl = PartitionPruner.prune(
          op.table,
          work.pctx.getOpToPartPruner().get(op.hiveOp),
          work.pctx.getConf(), "",
          work.pctx.getPrunedPartitions())
        op.parts = ppl.getConfirmedPartns.toArray ++ ppl.getUnknownPartns.toArray
        val allParts = op.parts ++ ppl.getDeniedPartns.toArray
        if (allParts.size == 0) {
          op.firstConfPartDesc = new PartitionDesc(op.tableDesc, null)
        } else {
          op.firstConfPartDesc = Utilities.getPartitionDesc(allParts(0).asInstanceOf[Partition])
        }
       }
    }
  }

  def initializeAllHiveOperators(terminalOp: TerminalOperator) {
    // Need to guarantee all parents are initialized before the child.
    val topOpList = new scala.collection.mutable.MutableList[HiveTopOperator]
    val queue = new scala.collection.mutable.Queue[Operator[_]]
    queue.enqueue(terminalOp)

    while (!queue.isEmpty) {
      val current = queue.dequeue()
      current match {
        case op: HiveTopOperator => topOpList += op
        case _ => Unit
      }
      queue ++= current.parentOperators
    }

    // Run the initialization. This guarantees that upstream operators are
    // initialized before downstream ones.
    topOpList.reverse.foreach { topOp =>
      topOp.initializeHiveTopOperator() 
    }
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}

