package shark

import java.io.FileOutputStream
import java.io.IOException
import java.io.PrintStream
import java.io.UnsupportedEncodingException
import java.util.ArrayList
import java.util.{List => JavaList}
import java.util.concurrent.CountDownLatch

import scala.annotation.tailrec
import scala.concurrent.ops.spawn

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.service.HiveServer.HiveServerHandler
import org.apache.hadoop.hive.service.HiveServer.ThriftHiveProcessorFactory
import org.apache.hadoop.hive.service.HiveServerException
import org.apache.hadoop.hive.service.ThriftHive
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportFactory

import spark.SparkEnv


/**
 * A long-running server compatible with the Hive server.
 */
object SharkServer extends LogHelper {

  // Force initialization of SharkEnv.
  var sparkEnv: SparkEnv = SparkEnv.get
  SharkEnv.init()

  @volatile
  var server: TThreadPoolServer = null

  var serverTransport: TServerSocket = _

  def main(args: Array[String]) {
    LogUtils.initHiveLog4j()

    var port = 10000
    var minWorkerThreads = 100
    var loadRdds = false

    @tailrec
    def parseArgs(iargs: Array[String]) {
      if (!iargs.isEmpty) iargs match {
        case Array("-p", y, _*) => port = y.toInt ; parseArgs(iargs.drop(2))
        case Array("-minWorkers", y, _*) => minWorkerThreads = y.toInt ; parseArgs(iargs.drop(2))
        case Array("-loadRdds", _*) => loadRdds = true ; parseArgs(iargs.drop(1))
        case _ => throw new Exception("Unsupported argument :" + iargs(0))
      }
    }
    parseArgs(args)

    val latch = new CountDownLatch(1)

    serverTransport = new TServerSocket(port)
    val hfactory = new ThriftHiveProcessorFactory(null, new HiveConf()) {
      override def getProcessor(t: TTransport) =
        new ThriftHive.Processor(new GatedSharkServerHandler(latch))
    }
    val ttServerArgs = new TThreadPoolServer.Args(serverTransport)
    ttServerArgs.processorFactory(hfactory)
    ttServerArgs.minWorkerThreads(minWorkerThreads)
    ttServerArgs.transportFactory(new TTransportFactory())
    ttServerArgs.protocolFactory(new TBinaryProtocol.Factory())
    server = new TThreadPoolServer(ttServerArgs)

    // Stop the server and clean up the Shark environment when we exit
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          if (server != null) {
            server.stop
            serverTransport.close
            server = null
            SharkEnv.stop()
          }
        }
      }
    )
    execLoadRdds(loadRdds, latch)
    logInfo("Starting shark server on port " + port)
    server.serve()
  }

  def ready: Boolean = if (server == null) false else server.isServing()

  private def execLoadRdds(loadFlag: Boolean, latch:CountDownLatch) {
    if (!loadFlag) {
      latch.countDown
    } else spawn {
      while (!server.isServing()) {}
      try {
        val sshandler = new SharkServerHandler
        CachedTableRecovery.loadAsRdds(sshandler.execute(_))
        logInfo("Executed load " + CachedTableRecovery.getMeta)
      } catch {
        case (e: Exception) => logWarning("Unable to load RDDs upon startup", e)
      } finally {
        latch.countDown
      }
    }
  }
}


class GatedSharkServerHandler(latch:CountDownLatch) extends SharkServerHandler {
  override def execute(cmd: String): Unit = {
    latch.await
    super.execute(cmd)
  }
}


class SharkServerHandler extends HiveServerHandler with LogHelper {

  private val ss = SessionState.get()

  private val conf: Configuration = if (ss != null) ss.getConf() else new Configuration()

  SharkConfVars.initializeWithDefaults(conf)

  private val driver = {
    val d = new SharkDriver(conf.asInstanceOf[HiveConf])
    d.init()
    d
  }

  // Make sure the ThreadLocal SparkEnv reference is the same for all threads.
  SparkEnv.set(SharkServer.sparkEnv)

  private var isSharkQuery = false

  override def execute(cmd: String) {
    SessionState.get()
    val cmd_trimmed = cmd.trim()
    val tokens = cmd_trimmed.split("\\s")
    val cmd_1 = cmd_trimmed.substring(tokens.apply(0).length()).trim()
    var response: Option[CommandProcessorResponse] = None

    val proc = CommandProcessorFactory.get(tokens.apply(0))
    if (proc != null) {
      if (proc.isInstanceOf[Driver]) {
        isSharkQuery = true
        proc.asInstanceOf[Driver].destroy()
        response = Option(driver.run(cmd))
      } else {
        isSharkQuery = false
        // Need to reset output for each non-Shark query.
        setupSessionIO(ss)
        response = Option(proc.run(cmd_1))
      }
    }

    response match {
      case Some(resp: CommandProcessorResponse) => {
        val code = resp.getResponseCode
        if (code != 0) throw new HiveServerException("Query returned non-zero code: " + code
          + ", cause: " + resp.getErrorMessage, code, resp.getSQLState)
      }
      case None => new HiveServerException
    }
  }

  // Called once per non-Shark query.
  def setupSessionIO(session: SessionState) {
    try {
      val tmpOutputFile = session.getTmpOutputFile()
      logInfo("Putting temp output to file " + tmpOutputFile.toString())
      // Open a per-session/command file for writing temp results.
      session.out = new PrintStream(new FileOutputStream(tmpOutputFile), true, "UTF-8")
      session.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: IOException => {
        try {
          session.in = null
          session.out = new PrintStream(System.out, true, "UTF-8")
          session.err = new PrintStream(System.err, true, "UTF-8")
        } catch {
          case ee: UnsupportedEncodingException => {
            ee.printStackTrace()
            session.out = null
            session.err = null
          }
        }
      }
    }
  }

  override def fetchAll(): JavaList[String] = {
    val res = new ArrayList[String]()
    if (isSharkQuery) {
      driver.getResults(res)
      res
    } else {
      // Returns all results if second arg (numRows) <= 0
      super.fetchAll()
    }
  }

  override def fetchN(numRows: Int): JavaList[String] = {
    val res = new ArrayList[String]()
    if (isSharkQuery) {
      driver.setMaxRows(numRows)
      driver.getResults(res)
      res
    } else {
      super.fetchN(numRows)
    }
  }

  override def fetchOne(): String = {
    if (!isSharkQuery) {
      ""
    } else {
      val list: JavaList[String] = fetchN(1)
      if (list.isEmpty)
        ""
      else list.get(0)
    }
  }

  override def getSchema: Schema = {
    if (!isSharkQuery) {
      new Schema
    } else {
      val schema: Schema = driver.getSchema
      if (schema == null) {
        new Schema
      } else {
        schema
      }
    }
  }

  override def getThriftSchema: Schema = {
    if (!isSharkQuery) {
      new Schema
    } else {
      val schema: Schema = driver.getThriftSchema
      if (schema == null) {
        new Schema
      } else {
        schema
      }
    }
  }
}
