/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark

import scala.language.existentials

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf


object SharkConfVars {

  val EXEC_MODE = new ConfVar("shark.exec.mode", "shark")

  // This is created for testing. Hive's test script assumes a certain output
  // format. To pass the test scripts, we need to use Hive's EXPLAIN.
  val EXPLAIN_MODE = new ConfVar("shark.explain.mode", "shark")

  // If true, keys that are NULL are equal. For strict SQL standard, set this to true.
  val JOIN_CHECK_NULL = new ConfVar("shark.join.checknull", true)

  val COLUMNAR_COMPRESSION = new ConfVar("shark.column.compress", true)

  // If true, then cache any table whose name ends in "_cached".
  val CHECK_TABLENAME_FLAG = new ConfVar("shark.cache.flag.checkTableName", true)

  // Specify the initial capacity for ArrayLists used to represent columns in columnar
  // cache. The default -1 for non-local mode means that Shark will try to estimate
  // the number of rows by using: partition_size / (num_columns * avg_field_size).
  val COLUMN_BUILDER_PARTITION_SIZE = new ConfVar("shark.column.partitionSize.mb",
    if (System.getenv("MASTER") == null) 1 else -1)

  // Prune map splits for cached tables based on predicates in queries.
  val MAP_PRUNING = new ConfVar("shark.mappruning", true)

  // Print debug information for map pruning.
  val MAP_PRUNING_PRINT_DEBUG = new ConfVar("shark.mappruning.debug", false)

  // If true, then query plans are compressed before being sent
  val COMPRESS_QUERY_PLAN = new ConfVar("shark.queryPlan.compress", true)

  // Number of mappers to force for table scan jobs
  val NUM_MAPPERS = new ConfVar("shark.map.tasks", -1)
  
  // Add Shark configuration variables and their default values to the given conf,
  // so default values show up in 'set'.
  def initializeWithDefaults(conf: Configuration) {
    if (conf.get(EXEC_MODE.varname) == null) {
      conf.set(EXEC_MODE.varname, EXEC_MODE.defaultVal)
    }
    if (conf.get(EXPLAIN_MODE.varname) == null) {
      conf.set(EXPLAIN_MODE.varname, EXPLAIN_MODE.defaultVal)
    }
    if (conf.get(COLUMN_BUILDER_PARTITION_SIZE.varname) == null) {
      conf.setInt(COLUMN_BUILDER_PARTITION_SIZE.varname,
        COLUMN_BUILDER_PARTITION_SIZE.defaultIntVal)
    }
    if (conf.get(COLUMNAR_COMPRESSION.varname) == null) {
      conf.setBoolean(COLUMNAR_COMPRESSION.varname, COLUMNAR_COMPRESSION.defaultBoolVal)
    }
    if (conf.get(CHECK_TABLENAME_FLAG.varname) == null) {
      conf.setBoolean(CHECK_TABLENAME_FLAG.varname, CHECK_TABLENAME_FLAG.defaultBoolVal)
    }
    if (conf.get(COMPRESS_QUERY_PLAN.varname) == null) {
      conf.setBoolean(COMPRESS_QUERY_PLAN.varname, COMPRESS_QUERY_PLAN.defaultBoolVal)
    }
    if (conf.get(MAP_PRUNING.varname) == null) {
      conf.setBoolean(MAP_PRUNING.varname, MAP_PRUNING.defaultBoolVal)
    }
    if (conf.get(MAP_PRUNING_PRINT_DEBUG.varname) == null) {
      conf.setBoolean(MAP_PRUNING_PRINT_DEBUG.varname, MAP_PRUNING_PRINT_DEBUG.defaultBoolVal)
    }
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

  def getIntVar(conf: Configuration, variable: HiveConf.ConfVars): Int = {
    HiveConf.getIntVar(conf, variable)
  }

  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars): Long = {
    HiveConf.getLongVar(conf, variable)
  }

  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Long): Long = {
    HiveConf.getLongVar(conf, variable, defaultVal)
  }

  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars): Float = {
    HiveConf.getFloatVar(conf, variable)
  }

  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Float): Float = {
    HiveConf.getFloatVar(conf, variable, defaultVal)
  }

  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars): Boolean = {
    HiveConf.getBoolVar(conf, variable)
  }

  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Boolean): Boolean = {
    HiveConf.getBoolVar(conf, variable, defaultVal)
  }

  def getVar(conf: Configuration, variable: HiveConf.ConfVars): String = {
    HiveConf.getVar(conf, variable)
  }

  def getVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: String): String = {
    HiveConf.getVar(conf, variable, defaultVal)
  }
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
    this(varname, classOf[Int], defaultVal.toString, defaultVal, 0, 0, false)
  }

  def this(varname: String, defaultVal: Long) = {
    this(varname, classOf[Long], defaultVal.toString, 0, defaultVal, 0, false)
  }

  def this(varname: String, defaultVal: Float) = {
    this(varname, classOf[Float], defaultVal.toString, 0, 0, defaultVal, false)
  }

  def this(varname: String, defaultVal: Boolean) = {
    this(varname, classOf[Boolean], defaultVal.toString, 0, 0, 0, defaultVal)
  }
}
