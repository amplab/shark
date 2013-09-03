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

import java.io.PrintStream

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.Logging

/**
 * Utility trait for classes that want to log data. This wraps around Spark's
 * Logging trait. It creates a SLF4J logger for the class and allows logging
 * messages at different levels using methods that only evaluate parameters
 * lazily if the log level is enabled.
 *
 * It differs from the Spark's Logging trait in that it can print out the
 * error to the specified console of the Hive session.
 */
trait LogHelper extends Logging {

  override def logError(msg: => String) = {
    errStream().println(msg)
    super.logError(msg)
  }

  def logError(msg: String, detail: String) = {
    errStream().println(msg)
    super.logError(msg + StringUtils.defaultString(detail))
  }

  def logError(msg: String, exception: Throwable) = {
    val err = errStream()
    err.println(msg)
    exception.printStackTrace(err)
    super.logError(msg, exception)
  }

  def outStream(): PrintStream = {
    val ss = SessionState.get()
    if (ss != null && ss.out != null) ss.out else System.out
  }

  def errStream(): PrintStream = {
    val ss = SessionState.get();
    if (ss != null && ss.err != null) ss.err else System.err
  }


}
