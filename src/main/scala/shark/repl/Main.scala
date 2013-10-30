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

package shark.repl

import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException


/**
 * Shark's REPL entry point.
 */
object Main {

  try {
    LogUtils.initHiveLog4j()
  } catch {
    case e: LogInitializationException => // Ignore the error.
  }

  private var _interp: SharkILoop = null

  def interp = _interp

  private def interp_=(i: SharkILoop) { _interp = i }

  def main(args: Array[String]) {

    _interp = new SharkILoop

    // We need to set spark.repl.InterpAccessor.interp since it is used
    // everywhere in spark.repl code.
    org.apache.spark.repl.Main.interp = _interp

    // Start an infinite loop ...
    _interp.process(args)
  }
}
