package shark

import java.util.{ArrayList => JavaArrayList, List => JavaList, Date}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.{Context, Driver, QueryPlan}
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.exec.OperatorFactory.OpTuple
import org.apache.hadoop.hive.ql.metadata.AuthorizationException
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.util.StringUtils

import scala.collection.JavaConversions._

import shark.exec.{SharkExplainTask, SharkExplainWork, SparkTask, SparkWork, TableRDD}
import shark.parse.{QueryContext, SharkSemanticAnalyzerFactory}


/**
 * This static object is responsible for two things:
 * 1. Replace OperatorFactory.opvec with Shark specific operators.
 * 2. Add Shark specific tasks to TaskFactory.taskvec.
 * 
 * See below for the SharkDriver class.
 */
object SharkDriver extends LogHelper {
  
  /**
   * A dummy static method so we can make sure the following static code are
   * executed.
   */
  def runStaticCode() {
    logInfo("Initializing object SharkDriver")
  }

  org.apache.hadoop.hive.serde2.SerDeUtils.registerSerDe(
    classOf[ColumnarSerDe].getName, classOf[ColumnarSerDe])

  // Task factory. Add Shark specific tasks.
  TaskFactory.taskvec.addAll(Seq(
    new TaskFactory.taskTuple(classOf[SparkWork], classOf[SparkTask]),
    new TaskFactory.taskTuple(classOf[SharkExplainWork], classOf[SharkExplainTask])))

  // Start the dashboard. Disabled by default. This was developed for the demo
  // at SIGMOD. We might turn it on later for general consumption.
  //dashboard.Dashboard.start()
}


/**
 * The driver to execute queries in Shark.
 */
class SharkDriver(conf: HiveConf) extends Driver(conf) with LogHelper {
  
  // Use reflection to make some private members accessible.
  val planField = this.getClass.getSuperclass.getDeclaredField("plan")
  val contextField = this.getClass.getSuperclass.getDeclaredField("ctx")
  val schemaField = this.getClass.getSuperclass.getDeclaredField("schema")
  contextField.setAccessible(true)
  planField.setAccessible(true)
  schemaField.setAccessible(true)
  
  val doAuthMethod = this.getClass.getSuperclass.getDeclaredMethod(
    "doAuthorization", classOf[BaseSemanticAnalyzer])
  doAuthMethod.setAccessible(true)
  val saHooksMethod = this.getClass.getSuperclass.getDeclaredMethod(
    "getSemanticAnalyzerHooks")
  saHooksMethod.setAccessible(true)
  
  // Helper methods to access the private members made accessible using reflection.
  def plan = getPlan
  def plan_= (value: QueryPlan): Unit = planField.set(this, value)

  def context = contextField.get(this).asInstanceOf[QueryContext]
  def context_= (value: QueryContext): Unit = contextField.set(this, value)

  def schema = schemaField.get(this).asInstanceOf[Schema]
  def schema_= (value: Schema): Unit = schemaField.set(this, value)
  
  var useTableRddSink = false

  override def init(): Unit = {
    // Forces the static code in SharkDriver to execute.
    SharkDriver.runStaticCode()
    
    // Init Hive Driver.
    super.init()
  }
  
  def tableRdd(cmd:String): TableRDD = {
    useTableRddSink = true
    val response = run(cmd)
    useTableRddSink = false
    plan.getRootTasks.get(0) match {
      case sparkTask: SparkTask => {
        sparkTask.tableRdd
      }
      case _ => null
    }
  }

  /**
   * Overload compile to use Shark's semantic analyzers.
   */
  override def compile(cmd: String): Int = {
    
    val now = new Date().getTime
    
        
    if (plan != null) {
      close()
      plan = null
    }
    
    TaskFactory.resetId()
    
    try {
      val command = new VariableSubstitution().substitute(conf, cmd)
      context = new QueryContext(conf, useTableRddSink)
      val tree = ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command, context))
      val sem = SharkSemanticAnalyzerFactory.get(conf, tree)
      
      // Do semantic analysis and plan generation
      val saHooks = saHooksMethod.invoke(this).asInstanceOf[JavaList[AbstractSemanticAnalyzerHook]]
      if (saHooks != null) {
        val hookCtx = new HiveSemanticAnalyzerHookContextImpl()
        hookCtx.setConf(conf);
        saHooks.foreach(_.preAnalyze(hookCtx, tree))
        sem.analyze(tree, context)
        saHooks.foreach(_.postAnalyze(hookCtx, sem.getRootTasks()))
      } else {
        sem.analyze(tree, context)
      }

      logInfo("Semantic Analysis Completed")
      
      sem.validate()
      
      plan = new QueryPlan(command, sem, now)
      
      // Initialize FetchTask right here. Somehow Hive initializes it twice...
      if (sem.getFetchTask != null) {
        sem.getFetchTask.initialize(conf, null, null)
      }
      
      // get the output schema
      schema = Driver.getSchema(sem, conf)
      
      // skip the testing serialization code
      
      // do the authorization check
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
        try {
          // Use reflection to invoke doAuthorization().
          doAuthMethod.invoke(this, sem)
        } catch {
          case authExp: AuthorizationException => {
            logError("Authorization failed:" + authExp.getMessage()
              + ". Use show grant to get more details.")
            return 403
          }
        }
      }

      // Success!
      0
    } catch {
      case e: SemanticException => {
        val errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage()
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        10
      }
      case e: ParseException => {
        val errorMessage = "FAILED: Parse Error: " + e.getMessage()
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        11
      }
      case e: Exception => {
        val errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e)
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        12
      } 
    }
  }
  
}

