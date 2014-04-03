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

package shark

import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.PrintStream
import java.io.UnsupportedEncodingException
import java.net.URLClassLoader
import java.util.ArrayList

import scala.collection.JavaConversions._

import jline.{History, ConsoleReader}

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.{HiveInterruptCallback, HiveInterruptUtils, LogUtils}
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.io.IOUtils
import org.apache.thrift.transport.TSocket


import shark.memstore2.TableRecovery

object SharkCliDriver {
  val SKIP_RDD_RELOAD_FLAG = "-skipRddReload"

  private var prompt  = "shark"
  private var prompt2 = "     " // when ';' is not yet seen.
  private var transport:TSocket = _

  installSignalHandler()

  /**
   * Install an interrupt callback to cancel all Spark jobs. In Hive's CliDriver#processLine(),
   * a signal handler will invoke this registered callback if a Ctrl+C signal is detected while
   * a command is being processed by the current thread.
   */
  def installSignalHandler() {
    HiveInterruptUtils.add(new HiveInterruptCallback {
      override def interrupt() {
        // Handle remote execution mode
        if (SharkEnv.sc != null) {
          SharkEnv.sc.cancelAllJobs()
        } else {
          if (transport != null) {
            // Force closing of TCP connection upon session termination
            transport.getSocket().close()
          }
        }
      }
    })
  }

  def main(args: Array[String]) {
    val hiveArgs = args.filterNot(_.equals(SKIP_RDD_RELOAD_FLAG))
    val reloadRdds = hiveArgs.length == args.length
    val oproc = new OptionsProcessor()
    if (!oproc.process_stage1(hiveArgs)) {
      System.exit(1)
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    var logInitFailed = false
    var logInitDetailMessage: String = null
    try {
      logInitDetailMessage = LogUtils.initHiveLog4j()
    } catch {
      case e: LogInitializationException =>
        logInitFailed = true
        logInitDetailMessage = e.getMessage()
    }

    val ss = new CliSessionState(new HiveConf(classOf[SessionState]))
    ss.in = System.in
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8")
      ss.info = new PrintStream(System.err, true, "UTF-8")
      ss.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(ss)) {
      System.exit(2)
    }

    if (!ss.getIsSilent()) {
      if (logInitFailed) System.err.println(logInitDetailMessage)
      else SessionState.getConsole().printInfo(logInitDetailMessage)
    }

    // Set all properties specified via command line.
    val conf: HiveConf = ss.getConf()
    ss.cmdProperties.entrySet().foreach { item: java.util.Map.Entry[Object, Object] =>
      conf.set(item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
      ss.getOverriddenConfigurations().put(
        item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
    }

    SessionState.start(ss)

    // Clean up after we exit
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          SharkEnv.stop()
        }
      }
    )

    // "-h" option has been passed, so connect to Shark Server.
    if (ss.getHost() != null) {
      ss.connect()
      if (ss.isRemoteMode()) {
        prompt = "[" + ss.getHost + ':' + ss.getPort + "] " + prompt
        val spaces = Array.tabulate(prompt.length)(_ => ' ')
        prompt2 = new String(spaces)
      }
    }

    if (!ss.isRemoteMode() && !ShimLoader.getHadoopShims().usesJobShell()) {
      // Hadoop-20 and above - we need to augment classpath using hiveconf
      // components.
      // See also: code in ExecDriver.java
      var loader = conf.getClassLoader()
      val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","))
      }
      conf.setClassLoader(loader)
      Thread.currentThread().setContextClassLoader(loader)
    }

    val cli = new SharkCliDriver(reloadRdds)
    cli.setHiveVariables(oproc.getHiveVariables())

    SharkEnv.fixUncompatibleConf(conf)

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

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    CliDriver.getCommandCompletor().foreach((e) => reader.addCompletor(e))

    var line: String = null
    val HISTORYFILE = ".hivehistory"
    val historyDirectory = System.getProperty("user.home")
    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + HISTORYFILE
        reader.setHistory(new History(new File(historyFile)))
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                           "history file.  History will not be available during this session.")
        System.err.println(e.getMessage())
    }

    // Use reflection to get access to the two fields.
    val getFormattedDbMethod = classOf[CliDriver].getDeclaredMethod(
      "getFormattedDb", classOf[HiveConf], classOf[CliSessionState])
    getFormattedDbMethod.setAccessible(true)

    val spacesForStringMethod = classOf[CliDriver].getDeclaredMethod(
      "spacesForString", classOf[String])
    spacesForStringMethod.setAccessible(true)

    val clientTransportTSocketField = classOf[CliSessionState].getDeclaredField("transport")
    clientTransportTSocketField.setAccessible(true)

    transport = clientTransportTSocketField.get(ss).asInstanceOf[TSocket]

    var ret = 0

    var prefix = ""
    val curDB = getFormattedDbMethod.invoke(null, conf, ss).asInstanceOf[String]
    var curPrompt = SharkCliDriver.prompt + curDB
    var dbSpaces = spacesForStringMethod.invoke(null, curDB).asInstanceOf[String]

    line = reader.readLine(curPrompt + "> ")
    while (line != null) {
      if (!prefix.equals("")) {
        prefix += '\n'
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        ret = cli.processLine(line, true)
        prefix = ""
        val sharkMode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE) == "shark"
        curPrompt = if (sharkMode) {
          SharkCliDriver.prompt
        } else {
          conf.getVar(HiveConf.ConfVars.CLIPROMPT)
        }
      } else {
        prefix = prefix + line
        val mode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE)
        curPrompt = if (mode == "shark") {
          SharkCliDriver.prompt2
        } else {
          spacesForStringMethod.invoke(null, mode).asInstanceOf[String]
        }
        curPrompt += dbSpaces
      }
      line = reader.readLine(curPrompt + "> ")
    }

    ss.close()

    System.exit(ret)
  } // end of main
}


class SharkCliDriver(reloadRdds: Boolean = true) extends CliDriver with LogHelper {

  private val ss = SessionState.get().asInstanceOf[CliSessionState]

  private val LOG = LogFactory.getLog("CliDriver")

  private val console = new SessionState.LogHelper(LOG)

  private val conf: Configuration = if (ss != null) ss.getConf() else new Configuration()

  SharkConfVars.initializeWithDefaults(conf)

  // Force initializing SharkEnv. This is put here but not object SharkCliDriver
  // because the Hive unit tests do not go through the main() code path.
  if (!ss.isRemoteMode()) {
    SharkEnv.init()
    if (reloadRdds) {
      console.printInfo(
        "Reloading cached RDDs from previous Shark sessions... (use %s flag to skip reloading)"
        .format(SharkCliDriver.SKIP_RDD_RELOAD_FLAG))
      TableRecovery.reloadRdds(processCmd(_), Some(console))
    }
  }

  def this() = this(false)

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
      tokens(0).toLowerCase().equals("list") ||
      ss.asInstanceOf[CliSessionState].isRemoteMode()) {
      val start = System.currentTimeMillis()
      super.processCmd(cmd)
      val end = System.currentTimeMillis()
      val timeTaken: Double = (end - start) / 1000.0
      console.printInfo("Time taken (including network latency): " + timeTaken + " seconds")
    } else {
      val hconf = conf.asInstanceOf[HiveConf]
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hconf)
      if (proc != null) {

        // Spark expects the ClassLoader to be an URLClassLoader.
        // In case we're using something else here, wrap it into an URLCLassLaoder.
        if (System.getenv("TEST_WITH_ANT") == "1") {
          val cl = Thread.currentThread.getContextClassLoader()
          Thread.currentThread.setContextClassLoader(new URLClassLoader(Array(), cl))
        }

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
            // Print the column names.
            val fieldSchemas = qp.getSchema.getFieldSchemas
            if (fieldSchemas != null) {
              out.println(fieldSchemas.map(_.getName).mkString("\t"))
            }
          }

          try {
            while (!out.checkError() && qp.getResults(res)) {
              res.foreach(line => out.println(line))
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

          // Destroy the driver to release all the locks.
          if (qp.isInstanceOf[SharkDriver]) {
            qp.destroy()
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

  override def processFile(fileName: String): Int = {
    if (Utils.isS3File(fileName)) {
      // For S3 file, fetch it from S3 and pass it to Hive.
      val conf = ss.getConf()
      Utils.setAwsCredentials(conf)
      var bufferReader: BufferedReader = null
      var rc: Int = 0
      try {
        bufferReader = Utils.createReaderForS3(fileName, conf)
        rc = processReader(bufferReader)
        bufferReader.close()
        bufferReader = null
      } finally {
        IOUtils.closeStream(bufferReader)
      }
      rc
    } else {
      // For non-S3 file, just use Hive's processFile.
      super.processFile(fileName)
    }
  }
}
