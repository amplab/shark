package org.apache.hadoop.hive.ql.exec

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration;


/**
 * A helper class that gets us PathFinder and alias in ScriptOperator.
 * This is needed since PathFinder inner class is not declared as
 * static/public.
 */
class ScriptOperatorHelper(val op: ScriptOperator) extends ScriptOperator {
  
  def newPathFinderInstance(envpath: String): op.PathFinder = {
    new op.PathFinder(envpath)
  }

  def getAlias: String = op.alias

  def addJobConfToEnvironment(conf: Configuration, env: JMap[String, String]) {
    ScriptOperator.addJobConfToEnvironment(conf, env)
  }

  def safeEnvVarName(variable: String): String = {
    ScriptOperator.safeEnvVarName(variable)
  }
}
