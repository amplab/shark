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

package org.apache.hadoop.hive.ql.exec
// Put this file in Hive's exec package to access package level visible fields and methods.

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration


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

  override def addJobConfToEnvironment(conf: Configuration, env: JMap[String, String]) {
    op.addJobConfToEnvironment(conf, env)
  }

  override def safeEnvVarName(variable: String): String = {
    op.safeEnvVarName(variable)
  }
}
