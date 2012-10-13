package shark

import spark.SparkContext
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.conf.HiveConf
import scala.collection.mutable.HashSet

/** A singleton object for the master program. The slaves should not access this. */
object SharkEnv extends LogHelper {

  /** A dummy method so we can make sure the following static code are executed.
   * This method is idempotent so it can be called multiple times.
   */
  def init() {}

  logInfo("Initializing SharkEnv")

  System.setProperty("spark.serializer", classOf[spark.KryoSerializer].getName)
  System.setProperty("spark.kryo.registrator", classOf[KryoRegistrator].getName)

  // Use Kryo to serialize closures. This is too buggy to be used.
  //System.setProperty("spark.closure.serializer", "spark.KryoSerializer")

  var sc: SparkContext = new SparkContext(
    if (System.getenv("MASTER") == null) "local" else System.getenv("MASTER"),
    "Shark::" + java.net.InetAddress.getLocalHost.getHostName)

  sc.putExecutorEnv("SPARK_MEM", getEnv("SPARK_SLAVE_MEM"))
  sc.putExecutorEnv("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
  sc.putExecutorEnv("HIVE_HOME", getEnv("HIVE_HOME"))
  sc.putExecutorEnv("HIVE_DEV_HOME", getEnv("HIVE_DEV_HOME"))
  sc.putExecutorEnv("HADOOP_HOME", getEnv("HADOOP_HOME"))
  sc.putExecutorEnv("JAVA_HOME", getEnv("JAVA_HOME"))

  val cache: CacheManager = new CacheManager

  // The following line turns Kryo serialization debug log on. It is extremely chatty.
  //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_DEBUG)

  // Keeps track of added JARs and files so that we don't add them twice in
  // consecutive queries.
  val addedFiles = HashSet[String]()
  val addedJars = HashSet[String]()

  /**
   * Cleans up and shuts down the Shark environments.
   * Stops the SparkContext and drops cached tables.
  */
  def stop() {
    logInfo("Shutting down Shark Environment")
    // Drop cached tables
    val db = Hive.get(new HiveConf)
    SharkEnv.cache.getAllKeyStrings foreach { key =>
      logInfo("Dropping cached table " + key)
      db.dropTable("default", key, false, true)
    }
    // Stop the SparkContext
    if (SharkEnv.sc != null) {
      sc.stop()
    }
  }

  def getEnv(variable: String) =
    if (System.getenv(variable) == null) "" else System.getenv(variable)
}


/** A singleton object for the slaves. */
object SharkEnvSlave {
  /**
   * A lock for various operations in ObjectInspectorFactory. Methods in that
   * class uses a static objectInspectorCache object to cache the creation of
   * object inspectors. That object is not thread safe so we wrap all calls to
   * that object in a synchronized lock on this.
   */
  val objectInspectorLock: AnyRef = new Object()
}
