package shark

import shark.{SharkEnv, SharkDriver}

import org.apache.hadoop.hive.service.ThriftHive
import org.apache.hadoop.hive.service.HiveServer.{ThriftHiveProcessorFactory, HiveServerHandler}
import org.apache.thrift.transport.{TTransport, TTransportFactory, TServerSocket}
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.thrift.TProcessor
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.conf.HiveConf
import spark.SparkContext
import java.util.ArrayList
import org.apache.hadoop.hive.metastore.api.Schema

/**
 * Created with IntelliJ IDEA.
 * User: harsha
 * Date: 7/5/12
 * Time: 10:23 PM
 * To change this template use File | Settings | File Templates.
 */



object SharkServer {

  private val LOG = LogFactory.getLog("SharkServer")
  SharkEnv.sc = new SparkContext(
    if (System.getenv("MASTER") == null) "local" else System.getenv("MASTER"),
    "Shark::" + java.net.InetAddress.getLocalHost.getHostName)

  def main(args:Array[String]){
    SessionState.initHiveLog4j();

    var port = 10000;
    var minWorkerThreads = 100// default number of threads serving the Hive server
    if (args.length >= 1) port = Integer.parseInt(args.apply(0))
    if (args.length >= 2) minWorkerThreads = Integer.parseInt(args.apply(1))

    val serverTransport = new TServerSocket(port);
    val hfactory = new SharkHiveProcessingFactory(null)
    val options = new TThreadPoolServer.Options()
    options.minWorkerThreads = minWorkerThreads
    val server = new TThreadPoolServer(hfactory, serverTransport,
      new TTransportFactory(), new TTransportFactory(),
      new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options)
    LOG.info("Starting shark server on port " + port)
    server.serve()
  }
}

class SharkHiveProcessingFactory(processor:TProcessor) extends ThriftHiveProcessorFactory(processor){

  override def getProcessor(trans: TTransport) = new ThriftHive.Processor(new SharkServerHandler)
}

class SharkServerHandler extends HiveServerHandler {

  private val ss = SessionState.get()

  private val conf: Configuration = if (ss != null) ss.getConf() else new Configuration()

  private val driver = {
    val d =new SharkDriver(conf.asInstanceOf[HiveConf])
    d.init()
    d
  }

  private var isSharkQuery = false

  private val LOG = LogFactory.getLog("SharkServerHandler")


  override def execute(cmd: String) {
    SessionState.get();

    val cmd_trimmed = cmd.trim()
    val tokens = cmd_trimmed.split("\\s")
    val cmd_1 = cmd_trimmed.substring(tokens.apply(0).length()).trim()
    


    val proc = CommandProcessorFactory.get(tokens.apply(0))
    if (proc != null) {
      if (proc.isInstanceOf[Driver]) {
        isSharkQuery = true
        proc.asInstanceOf[Driver].destroy()
        driver.run(cmd)
      } else {
        isSharkQuery = false
        proc.run(cmd_1)
      }

    }
  }

  override def fetchAll() = {
    val res = new ArrayList[String]()
    if (isSharkQuery) {
      driver.getResults(res)
    }
    res
  }

  override def fetchN(numRows: Int) = {
    val res = new ArrayList[String]()
    if (isSharkQuery) {
      driver.setMaxRows(numRows)
      driver.getResults(res)
    }
    res
  }

  override def fetchOne() = {
    if(!isSharkQuery) {
      ""
    }else {
      val list = fetchN(1)
      if (list.isEmpty)
        ""
      else list.get(0)
    }
  }

  override def getSchema = {
    if(!isSharkQuery) {
      new Schema
    }else {
      val schema = driver.getSchema
      if (schema == null){
        new Schema
      }else{
        schema
      }
    }
  }

  override def getThriftSchema = {
    if(!isSharkQuery) {
      new Schema
    }else {
      val schema = driver.getThriftSchema
      if (schema == null){
        new Schema
      }else{
        schema
      }
    }
  }
}
