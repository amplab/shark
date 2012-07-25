package shark

import spark.SparkContext

import jline.{History, ConsoleReader}

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, Utilities}
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader


import java.io.{File, FileNotFoundException, IOException, PrintStream, UnsupportedEncodingException}
import java.util.ArrayList

import scala.collection.JavaConversions._


object SharkCliDriver {

  val prompt  = "shark"
  val prompt2 = "     " // when ';' is not yet seen

  // The testing script uses SharkCliDriver but doesn't go through the main
  // routine. We initialize SparkContext here. Make sure something in this
  // object is referenced to force initializing SparkContext.
  SharkEnv.sc = new SparkContext(
    if (System.getenv("MASTER") == null) "local" else System.getenv("MASTER"),
    "Shark::" + java.net.InetAddress.getLocalHost.getHostName)

  def main(args: Array[String]) {
    
    val oproc = new OptionsProcessor();
    if (!oproc.process_stage1(args)) {
      System.exit(1)
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    LogUtils.initHiveLog4j()

    var ss = new CliSessionState(new HiveConf(classOf[SessionState]))
    ss.in = System.in
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8")
      ss.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(ss)) {
      System.exit(2)
    }

    // set all properties specified via command line
    val conf:HiveConf = ss.getConf()
    ss.cmdProperties.entrySet().foreach { item: java.util.Map.Entry[Object, Object] =>
      conf.set(item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
    }

    // Drop cached tables from the metastore after we exit
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          val db = Hive.get(conf.asInstanceOf[HiveConf])
          SharkEnv.cache.getAllKeyStrings foreach { key =>
            db.dropTable("default", key, false, true)
          }
        }
      }
    )

    if (!ShimLoader.getHadoopShims().usesJobShell()) {
      // hadoop-20 and above - we need to augment classpath using hiveconf
      // components
      // see also: code in ExecDriver.java
      var loader = conf.getClassLoader()
      val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","))
      }
      conf.setClassLoader(loader)
      Thread.currentThread().setContextClassLoader(loader)
    }
    SessionState.start(ss)

    var cli = new SharkCliDriver()

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(ss)

    if (ss.execString != null) {
      System.exit(cli.processLine(ss.execString))
    }

    try {
      if (ss.fileName != null) {
        System.exit(cli.processFile(ss.fileName))
      }
    } catch {
      case e: FileNotFoundException => 
        System.err.println("Could not open input file for reading. (" + e.getMessage() + ")")
        System.exit(3)
    }

    var reader = new ConsoleReader()
    reader.setBellEnabled(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    reader.addCompletor(CliDriver.getCommandCompletor())

    var line:String = ""
    val HISTORYFILE = ".hivehistory"
    val historyFile = System.getProperty("user.home") + File.separator + HISTORYFILE
    reader.setHistory(new History(new File(historyFile)))
    var ret = 0

    var prefix = ""
    var curPrompt = SharkCliDriver.prompt
    line = reader.readLine(curPrompt + "> ")
    while (line != null) {
      if (!prefix.equals("")) {
        prefix += '\n'
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        ret = cli.processLine(line)
        prefix = ""
        curPrompt =
          if (SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark") {
            SharkCliDriver.prompt
          } else {
            CliDriver.prompt
          }
      } else {
        prefix = prefix + line
        curPrompt = 
          if (SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark") {
            SharkCliDriver.prompt2
          } else {
            CliDriver.prompt2
          }
      }
      line = reader.readLine(curPrompt + "> ")
    }

    System.exit(ret)
  }
}


class SharkCliDriver extends CliDriver with LogHelper {

  private val ss = SessionState.get()

  private val LOG = LogFactory.getLog("CliDriver")

  private val console = new SessionState.LogHelper(LOG)

  private val conf: Configuration = if (ss != null) ss.getConf() else new Configuration()

  // Force initializing the SparkContext in SharkCliDriver object.
  SharkCliDriver.prompt
  
  override def processCmd(cmd: String): Int = {
    val ss: SessionState = SessionState.get()
    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    var ret = 0

    if (cmd_trimmed.toLowerCase().equals("quit") ||
      cmd_trimmed.toLowerCase().equals("exit") ||
      tokens(0).equalsIgnoreCase("source") ||
      cmd_trimmed.startsWith("!") ||
      tokens(0).toLowerCase().equals("list")) {
      super.processCmd(cmd)
    } else {
      val hconf = conf.asInstanceOf[HiveConf]
      val proc = CommandProcessorFactory.get(tokens(0), hconf)
      if (proc != null) {
        if (proc.isInstanceOf[Driver]) {
          // There is a small overhead here to create a new instance of
          // SharkDriver for every command. But it saves us the hassle of
          // hacking CommandProcessorFactory.
          val qp: Driver =
            if (SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark") {
              new SharkDriver(hconf)
            } else {
              proc.asInstanceOf[Driver]
            }
          
          logInfo("Execution Mode: " + SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE))

          qp.init()
          val out = ss.out
          val start:Long = System.currentTimeMillis()
          if (ss.getIsVerbose()) {
            out.println(cmd)
          }

          ret = qp.run(cmd).getResponseCode()
          if (ret != 0) {
            qp.close()
            return ret
          }

          val res = new ArrayList[String]()
          
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
            // Print the column names
            val fieldSchemas = qp.getSchema.getFieldSchemas
            if (fieldSchemas != null) {
              out.println(fieldSchemas.map(_.getName).mkString("\t"))
            }
          }
          
          try {
            while (!out.checkError() && qp.getResults(res)) {
              res.foreach(out.println(_))
              res.clear()
            }
          } catch {
            case e:IOException =>
              console.printError("Failed with exception " + e.getClass().getName() + ":" +
                e.getMessage(), "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e))
              ret = 1
          }

          val cret = qp.close()
          if (ret == 0) {
            ret = cret
          }

          val end:Long = System.currentTimeMillis()
          if (end > start) {
            val timeTaken:Double = (end - start) / 1000.0
            console.printInfo("Time taken: " + timeTaken + " seconds", null)
          }

        } else {
          if (ss.getIsVerbose()) {
            ss.out.println(tokens(0) + " " + cmd_1)
          }
          ret = proc.run(cmd_1).getResponseCode()
        }
      }
    }
    ret
  }
}

