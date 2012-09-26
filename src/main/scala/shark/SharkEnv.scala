package shark

import shark.memstore.CacheManager
import spark.SparkContext

object SharkEnv extends LogHelper {

  /**
   * A dummy method so we can make sure the following static code are executed.
   */
  def init() {
    logDebug("Initializing object SharkEnv")
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

  /**
   * A lock for various operations in ObjectInspectorFactory. Methods in that
   * class uses a static objectInspectorCache object to cache the creation of
   * object inspectors. That object is not thread safe so we wrap all calls to
   * that object in a synchronized lock on this.
   */
  val objectInspectorLock: AnyRef = new Object()

}

