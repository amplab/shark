package shark

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}
import org.apache.hive.service.CompositeService
import org.apache.spark.SparkEnv
import shark.server.SharkCLIService

import scala.collection.JavaConversions._

import org.apache.spark.sql.hive.CatalystContext

/**
 * The main entry point for the Shark port of HiveServer2.  Starts up a CatalystContext and a SharkServer2 thrift server.
 */
object SharkServer2 extends Logging {
  var LOG = LogFactory.getLog(classOf[SharkServer2])

  def main(args: Array[String]) {
    val optproc = new ServerOptionsProcessor("sharkserver2")

    if (!optproc.process(args)) {
      logger.warn("Error starting SharkServer2 with given arguments")
      System.exit(-1)
    }

    val ss = new SessionState(new HiveConf(classOf[SessionState]))

    // Set all properties specified via command line.
    val hiveConf: HiveConf = ss.getConf()

    SessionState.start(ss)
    
    logger.info("Starting SparkContext")
    CatalystEnv.init()
    logger.info("Starting CatalystContext")
    SessionState.start(ss)

    //server.SharkServer.hiveContext = hiveContext

    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          CatalystEnv.sc.stop()
        }
      }
    )

    try {
      val server = new SharkServer2(CatalystEnv.cc)
      server.init(hiveConf)
      server.start()
      logger.info("SharkServer2 started")
    } catch {
      case e: Exception => {
        logger.error("Error starting SharkServer2", e)
        System.exit(-1)
      }
    }
  }
}

private[shark] class SharkServer2(catalystContext: CatalystContext) extends HiveServer2 {
  override def init(hiveConf: HiveConf): Unit = synchronized {
    val sharkCLIService = new SharkCLIService(catalystContext)
    Utils.setSuperField("cliService", sharkCLIService, this)
    addService(sharkCLIService)
    val sthriftCLIService = new ThriftBinaryCLIService(sharkCLIService)
    Utils.setSuperField("thriftCLIService", sthriftCLIService, this)
    addService(sthriftCLIService)
    sharkInit(hiveConf)
  }
}


