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

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

object CliTestToolkit {

  def getWarehousePath(prefix: String): String = {
    System.getProperty("user.dir") + "/test_warehouses/" + prefix + "-warehouse-" +
      (new SimpleDateFormat("yyyyMMdd-HHmmss")).format(new Date)
  }

  def getMetastorePath(prefix: String): String = {
    System.getProperty("user.dir") + "/test_warehouses/" + prefix + "-metastore-" +
      (new SimpleDateFormat("yyyyMMdd-HHmmss")).format(new Date)
  }
}

trait CliTestToolkit {

  var process : Process = null
  var outputWriter : PrintWriter = null
  var inputReader : BufferedReader = null
  var errorReader : BufferedReader = null

  def dropTable(tableName: String, timeout: Long = 15000): String = {
    executeQuery("drop table if exists " + tableName + ";")
  }

  def executeQuery(
    cmd: String, outputMessage: String = "OK", timeout: Long = 15000): String = {
    println("Executing: " + cmd + ", expecting output: " + outputMessage)
    outputWriter.write(cmd + "\n")
    outputWriter.flush()
    waitForQuery(timeout, outputMessage)
  }

  protected def waitForQuery(timeout: Long, message: String): String = {
    if (waitForOutput(errorReader, message, timeout)) {
      Thread.sleep(500)
      readOutput()
    } else {
      assert(false, "Didn't find \"" + message + "\" in the output:\n" + readOutput())
      null
    }
  }

  protected def waitForQuery(timeout: Long, message1: String, message2: String): String = {
    if (waitForOutput2(errorReader, message1, message2, timeout)) {
      Thread.sleep(500)
      readOutput()
    } else {
      assert(false, "Didn't find '" + message1 + "' or '" + message2 +
        "' in the output:\n" + readOutput())
      null
    }
  }

  // Wait for the specified str to appear in the output.
  protected def waitForOutput(
    reader: BufferedReader, str: String, timeout: Long = 10000): Boolean = {
    val startTime = System.currentTimeMillis
    var out = ""
    while (!out.contains(str) && (System.currentTimeMillis) < (startTime + timeout)) {
      out += readFrom(reader)
    }
    out.contains(str)
  }

  // Wait for the specified str1 and str2 to appear in the output.
  protected def waitForOutput2(
    reader: BufferedReader, str1: String, str2: String, timeout: Long = 10000): Boolean = {
    val startTime = System.currentTimeMillis
    var out = ""
    while (!out.contains(str1) && !out.contains(str2) &&
      (System.currentTimeMillis) < (startTime + timeout)) {
      out += readFrom(reader)
    }
    out.contains(str1) || out.contains(str2)
  }

  // Read stdout output from Shark and filter out garbage collection messages.
  protected def readOutput(): String = {
    val output = readFrom(inputReader)
    // Remove GC Messages
    val filteredOutput = output.lines.filterNot(x => x.contains("[GC") || x.contains("[Full GC"))
      .mkString("\n")
    filteredOutput
  }

  protected def readFrom(reader: BufferedReader): String = {
    var out = ""
    var c = 0
    while (reader.ready) {
      c = reader.read()
      out += c.asInstanceOf[Char]
    }
    print(out)
    out
  }
}
