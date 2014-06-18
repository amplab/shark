/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

  def logError(msg: => String) = {
    errStream().println(msg)
    logger.error(msg)
  }
  
  def logWarning(msg: => String) = {
    errStream().println(msg)
    logger.warn(msg)
  }
    
  def logInfo(msg: => String) = {
    errStream().println(msg)
    logger.info(msg)
  }

  def logDebug(msg: => String) = {
    errStream().println(msg)
    logger.debug(msg)
  }

  def logError(msg: String, detail: String) = {
    errStream().println(msg)
    logger.error(msg + StringUtils.defaultString(detail))
  }

  def logError(msg: String, exception: Throwable) = {
    val err = errStream()
    err.println(msg)
    exception.printStackTrace(err)
    logger.error(msg, exception)
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
