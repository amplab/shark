package shark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf


object SharkConfVars {
  
  val EXEC_MODE = new ConfVar("shark.exec.mode", "shark")
  
  // This is created for testing. Hive's test script assumes a certain output
  // format. To pass the test scripts, we need to use Hive's EXPLAIN.
  val EXPLAIN_MODE = new ConfVar("shark.explain.mode", "shark")

  // Specify the initial capacity for ArrayLists used to represent columns in columnar
  // cache. The default -1 for non-local mode means that Shark will try to estimate
  // the number of rows by using: partition_size / (num_columns * avg_field_size).
  val COLUMN_INITIALSIZE = new ConfVar("shark.columnar.cache.initialSize",
    if (System.getenv("MASTER") == null) 100 else -1)

  // If true, then cache any table whose name ends in "_cached".
  val CHECK_TABLENAME_FLAG = new ConfVar("shark.cache.flag.checkTableName", false)

  // If true, then query plans are compressed before being sent
  val COMPRESS_QUERY_PLAN = new ConfVar("shark.compressQueryPlan", true)

  // Add Shark configuration variables and their default values to the given conf,
  // so default values show up in 'set'.
  def initializeWithDefaults(conf: Configuration) {
    conf.set(EXEC_MODE.varname, EXEC_MODE.defaultVal)
    conf.set(EXPLAIN_MODE.varname, EXPLAIN_MODE.defaultVal)

    conf.setInt(COLUMN_INITIALSIZE.varname, COLUMN_INITIALSIZE.defaultIntVal)

    conf.setBoolean(CHECK_TABLENAME_FLAG.varname, CHECK_TABLENAME_FLAG.defaultBoolVal)
    conf.setBoolean(COMPRESS_QUERY_PLAN.varname, COMPRESS_QUERY_PLAN.defaultBoolVal)
  }
  
  def getIntVar(conf: Configuration, variable: ConfVar): Int = {
    require(variable.valClass == classOf[Int])
    conf.getInt(variable.varname, variable.defaultIntVal)
  }

  def getLongVar(conf: Configuration, variable: ConfVar): Long = {
    require(variable.valClass == classOf[Long])
    conf.getLong(variable.varname, variable.defaultLongVal)
  }

  def getFloatVar(conf: Configuration, variable: ConfVar): Float = {
    require(variable.valClass == classOf[Float])
    conf.getFloat(variable.varname, variable.defaultFloatVal)
  }
  
  def getBoolVar(conf: Configuration, variable: ConfVar): Boolean = {
    require(variable.valClass == classOf[Boolean])
    conf.getBoolean(variable.varname, variable.defaultBoolVal)
  }
  
  def getVar(conf: Configuration, variable: ConfVar): String = {
    require(variable.valClass == classOf[String])
    conf.get(variable.varname, variable.defaultVal)
  }

  def setVar(conf: Configuration, variable: ConfVar, value: String) {
    require(variable.valClass == classOf[String])
    conf.set(variable.varname, value)
  }

  def getIntVar(conf: Configuration, variable: HiveConf.ConfVars) 
    = HiveConf.getIntVar _
  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars) 
    = HiveConf.getLongVar(conf, variable)
  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Long) 
    = HiveConf.getLongVar(conf, variable, defaultVal)
  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars) 
    = HiveConf.getFloatVar(conf, variable)
  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Float) 
    = HiveConf.getFloatVar(conf, variable, defaultVal)
  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars) 
    = HiveConf.getBoolVar(conf, variable)
  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Boolean) 
    = HiveConf.getBoolVar(conf, variable, defaultVal)
  def getVar(conf: Configuration, variable: HiveConf.ConfVars) 
    = HiveConf.getVar(conf, variable)
  def getVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: String) 
    = HiveConf.getVar(conf, variable, defaultVal)
  
}


case class ConfVar(
  varname: String,
  valClass: Class[_],
  defaultVal: String,
  defaultIntVal: Int,
  defaultLongVal: Long,
  defaultFloatVal: Float,
  defaultBoolVal: Boolean) {

  def this(varname: String, defaultVal: String) = {
    this(varname, classOf[String], defaultVal, 0, 0, 0, false)
  }

  def this(varname: String, defaultVal: Int) = {
    this(varname, classOf[Int], null, defaultVal, 0, 0, false)
  }

  def this(varname: String, defaultVal: Long) = {
    this(varname, classOf[Long], null, 0, defaultVal, 0, false)
  }

  def this(varname: String, defaultVal: Float) = {
    this(varname, classOf[Float], null, 0, 0, defaultVal, false)
  }

  def this(varname: String, defaultVal: Boolean) = {
    this(varname, classOf[Boolean], null, 0, 0, 0, defaultVal)
  }
}
