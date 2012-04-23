package shark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf


object SharkConfVars {
  
  val EXEC_MODE = new ConfVar("shark.exec.mode", "shark")
  
  // This is created for testing. Hive's test script assumes a certain output
  // format. To pass the test scripts, we need to use Hive's EXPLAIN.
  val EXPLAIN_MODE = new ConfVar("shark.explain.mode", "shark")
  
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

  def getIntVar(conf: Configuration, variable: HiveConf.ConfVars) = HiveConf.getIntVar _
  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars) = HiveConf.getLongVar _
  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars) = HiveConf.getFloatVar _
  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars) = HiveConf.getBoolVar _
  def getVar(conf: Configuration, variable: HiveConf.ConfVars) = HiveConf.getVar _
  
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
    this(varname, classOf[String], null, defaultVal, 0, 0, false)
  }

  def this(varname: String, defaultVal: Long) = {
    this(varname, classOf[String], null, 0, defaultVal, 0, false)
  }

  def this(varname: String, defaultVal: Float) = {
    this(varname, classOf[String], null, 0, 0, defaultVal, false)
  }

  def this(varname: String, defaultVal: Boolean) = {
    this(varname, classOf[String], null, 0, 0, 0, defaultVal)
  }
}
