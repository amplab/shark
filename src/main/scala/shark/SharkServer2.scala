package shark

import scala.collection.JavaConversions._

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import shark.server.SharkCLIService

/**
 * The main entry point for the Shark port of HiveServer2.  Starts up a HiveContext and a SharkServer2 thrift server.
 */
object SharkServer2 extends Logging {
  var LOG = LogFactory.getLog(classOf[SharkServer2])

  def main(args: Array[String]) {
    val optproc = new ServerOptionsProcessor("sharkserver2")

    if (!optproc.process(args)) {
      logger.warn("Error starting SharkServer2 with given arguments")
      System.exit(-1)
    }

    logger.info("Starting SparkContext")
    val sparkContext = new SparkContext("local", "")
    logger.info("Starting HiveContext")
    val hiveContext = new HiveContext(sparkContext)

    //server.SharkServer.hiveContext = hiveContext

    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          sparkContext.stop()
        }
      }
    )

    try {
      val hiveConf = new HiveConf
      val server = new SharkServer2(hiveContext)
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

private[shark] class SharkServer2(hiveContext: HiveContext) extends HiveServer2 {
  override def init(hiveConf: HiveConf): Unit = synchronized {
    val sharkCLIService = new SharkCLIService(hiveContext)
    Utils.setSuperField("cliService", sharkCLIService, this)
    addService(sharkCLIService)
    val sthriftCLIService = new ThriftBinaryCLIService(sharkCLIService)
    Utils.setSuperField("thriftCLIService", sthriftCLIService, this)
    addService(sthriftCLIService)
    sharkInit(hiveConf)
  }
}
