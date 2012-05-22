package shark

import spark.SparkContext

object SharkEnv extends LogHelper {

  /**
   * A dummy static method so we can make sure the following static code are
   * executed.
   */
  def init() {
    logInfo("Initializing object SharkEnv")
  }

  System.setProperty("spark.serializer", classOf[spark.KryoSerializer].getName)
  System.setProperty("spark.kryo.registrator", classOf[KryoRegistrator].getName)

  // Use Kryo to serialize closures. This is too buggy to be used.
  //System.setProperty("spark.closure.serializer", "spark.KryoSerializer")

  // This is also set in SharkILoop in dynamic code. If this is moved, make
  // sure we change SharkILoop too.
  var sc: SparkContext = null

  val cache: CacheManager = new CacheManager

  // The following line turns Kryo serialization debug log on. It is extremely chatty.
  //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_DEBUG)

}

