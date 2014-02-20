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

import java.io.FileOutputStream
import java.io.IOException
import java.io.PrintStream
import java.io.UnsupportedEncodingException
import java.net.InetSocketAddress 
import java.util.ArrayList
import java.util.{List => JavaList}
import java.util.Properties
import java.util.concurrent.CountDownLatch

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.commons.logging.LogFactory
import org.apache.commons.cli.OptionBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.service.HiveServer.HiveServerCli
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
import org.apache.thrift.transport.TSocket

import org.apache.spark.SparkEnv

import shark.memstore2.TableRecovery


/**
 * A long-running server compatible with the Hive server.
 */
object SharkServer extends LogHelper {

  // Force initialization of SharkEnv.
  SharkEnv.init()
  var sparkEnv: SparkEnv = SparkEnv.get

  @volatile
  var server: TThreadPoolServer = null

  var serverTransport: TServerSocket = _

  def main(args: Array[String]) {

    val cliOptions = new SharkServerCliOptions
    cliOptions.parse(args)

    // From Hive: It is critical to do this prior to initializing log4j, otherwise
    // any log specific settings via hiveconf will be ignored.
    val hiveconf: Properties = cliOptions.addHiveconfToSystemProperties()

    SharkEnv.fixUncompatibleConf(new HiveConf())

    // From Hive: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    LogUtils.initHiveLog4j()

    val latch = new CountDownLatch(1)
    serverTransport = new TServerSocket(cliOptions.port)

    val hfactory = new ThriftHiveProcessorFactory(null, new HiveConf()) {
      override def getProcessor(t: TTransport) = {
        var remoteClient = "Unknown"

        // Seed session ID by a random number
        var sessionID = scala.math.round(scala.math.random * 10000000).toString
        var jdbcSocket: java.net.Socket = null
        if (t.isInstanceOf[TSocket]) {
          remoteClient = t.asInstanceOf[TSocket].getSocket()
            .getRemoteSocketAddress().asInstanceOf[InetSocketAddress]
            .getAddress().toString()
           
          jdbcSocket = t.asInstanceOf[TSocket].getSocket()
          jdbcSocket.setKeepAlive(true)
          sessionID = remoteClient + "/" + jdbcSocket 
            .getRemoteSocketAddress().asInstanceOf[InetSocketAddress].getPort().toString +
            "/" + sessionID

        }
        logInfo("Audit Log: Connection Initiated with JDBC client - " + remoteClient)
        
        // Add and enable watcher thread
        // This handles both manual killing of session as well as connection drops
        val watcher = new JDBCWatcher(jdbcSocket, sessionID)
        SharkEnv.activeSessions.add(sessionID)
        watcher.start()
        
        new ThriftHive.Processor(new GatedSharkServerHandler(latch, remoteClient, 
            sessionID))
      }
    }
    val ttServerArgs = new TThreadPoolServer.Args(serverTransport)
      .processorFactory(hfactory)
      .minWorkerThreads(cliOptions.minWorkerThreads)
      .maxWorkerThreads(cliOptions.maxWorkerThreads)
      .transportFactory(new TTransportFactory())
      .protocolFactory(new TBinaryProtocol.Factory())
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

    // Optionally reload cached tables from a previous session.
    execLoadRdds(cliOptions.reloadRdds, latch)

    // Start serving.
    val startupMsg = "Starting Shark server on port " + cliOptions.port + " with " +
      cliOptions.minWorkerThreads + " min worker threads and " + cliOptions.maxWorkerThreads +
      " max worker threads."
    logInfo(startupMsg)
    println(startupMsg)
    server.serve()
  }

  def stop() {
    server.stop()
    SharkEnv.stop()
  }

  // Return true if the server is ready to accept requests.
  def ready: Boolean = if (server == null) false else server.isServing()

  private def execLoadRdds(loadFlag: Boolean, latch:CountDownLatch) {
    if (!loadFlag) {
      latch.countDown
    } else future {
      while (!server.isServing()) {}
      try {
        val sshandler = new SharkServerHandler
        TableRecovery.reloadRdds(sshandler.execute(_))
      } catch {
        case (e: Exception) => logWarning("Unable to load RDDs upon startup", e)
      } finally {
        latch.countDown
      }
    }
  }
  
  // Detecting socket connection drops relies on TCP keep alives
  // The approach is very platform specific on the duration and nature of detection
  // Since java does not expose any mechanisms for tuning keepalive configurations,
  //  the users should explore the server OS settings for the same.
  class JDBCWatcher(sock:java.net.Socket, sessionID:String) extends Thread {
    
    override def run() {
      try {
        while ((sock == null || sock.isConnected) && SharkEnv.activeSessions.contains(sessionID)) {	    
          if (sock != null)
            sock.getOutputStream().write((new Array[Byte](0)).toArray)
          logDebug("Session Socket Alive - " + sessionID)
          Thread.sleep(2*1000)
        }
      } catch {
        case ioe: IOException => Unit
      }

      // Session is terminated either manually or automatically
      // clean up the jobs associated with the session ID
      logInfo("Session Socket connection lost, cleaning up - " + sessionID)
      SharkEnv.sc.cancelJobGroup(sessionID)
    }

  }

  // Used to parse command line arguments for the server.
  class SharkServerCliOptions extends HiveServerCli {
    var reloadRdds = false

    val OPTION_SKIP_RELOAD_RDDS = "skipRddReload"
    OPTIONS.addOption(OptionBuilder.create(OPTION_SKIP_RELOAD_RDDS))

    override def parse(args: Array[String]) {
      super.parse(args)
      reloadRdds = !commandLine.hasOption(OPTION_SKIP_RELOAD_RDDS)
    }
  }
}


class GatedSharkServerHandler(latch:CountDownLatch, remoteClient:String,
    sessionID:String) extends SharkServerHandler {
  override def execute(cmd: String): Unit = {
    latch.await
    
    logInfo("Audit Log: SessionID=" + sessionID + " client=" + remoteClient + "  cmd=" + cmd)
    
    // Handle cancel commands
    if (cmd.startsWith("kill ")) {
      logInfo("killing group - " + cmd)
      val sessionIDToCancel = cmd.split("\\s+|\\s*;").apply(1)
      SharkEnv.activeSessions.remove(sessionIDToCancel)
    } else {
      // Session ID is used as spark job group
      // Job groups control cleanup/cancelling of unneeded jobs on connection terminations
      SharkEnv.sc.setJobGroup(sessionID, "Session ID = " + sessionID)
      super.execute(cmd)
    }
  }
}


class SharkServerHandler extends HiveServerHandler with LogHelper {

  private val (sessionState: SessionState, conf: Configuration) = {
    if (SessionState.get() != null) {
      (SessionState.get(), SessionState.get().getConf())
    } else {
      val newConf = new Configuration()
      val ss = SessionState.start(newConf.asInstanceOf[HiveConf])
      setupSessionIO(ss)
      (ss,newConf)
    }
  }

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
    SessionState.start(sessionState)
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
        setupSessionIO(sessionState)
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
      if (list.isEmpty) {
        ""
      } else {
        list.get(0)
      }
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
