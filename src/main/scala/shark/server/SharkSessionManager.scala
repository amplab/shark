package shark.server

import org.apache.hive.service.cli.session.SessionManager
import org.apache.hadoop.hive.conf.HiveConf
import shark.Utils

class SharkSessionManager extends SessionManager {
  override def init(hiveConf : HiveConf) {
    this.synchronized {
      val sharkOpManager = new SharkOperationManager
      Utils.setSuperField("operationManager", sharkOpManager, this)
      addService(sharkOpManager)
      sharkInit(hiveConf)
    }

  }
}
