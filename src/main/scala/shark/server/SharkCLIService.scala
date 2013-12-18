package shark.server

import org.apache.hive.service.cli.CLIService
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hive.service.auth.HiveAuthFactory
import java.io.IOException
import org.apache.hive.service.ServiceException
import javax.security.auth.login.LoginException
import org.apache.spark.SparkEnv
import shark.{SharkServer, Utils}

class SharkCLIService extends CLIService {
  override def init(hiveConf: HiveConf) {
    this.synchronized {
      Utils.setSuperField("hiveConf", hiveConf, this)
      val sharkSM = new SharkSessionManager
      Utils.setSuperField("sessionManager", sharkSM, this)
      addService(sharkSM)
      try {
        HiveAuthFactory.loginFromKeytab(hiveConf)
        val serverUserName = ShimLoader.getHadoopShims
          .getShortUserName(ShimLoader.getHadoopShims.getUGIForConf(hiveConf))
        Utils.setSuperField("serverUserName", serverUserName, this)
      } catch {
        case e: IOException => {
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
        }
        case e: LoginException => {
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
        }
      }
      // Make sure the ThreadLocal SparkEnv reference is the same for all threads.
      SparkEnv.set(SharkServer.sparkEnv)
      sharkInit(hiveConf)
    }
  }
}


