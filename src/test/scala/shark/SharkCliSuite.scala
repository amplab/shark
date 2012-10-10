package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfter, FunSuite}

abstract class SharkCliSuite extends FunSuite with BeforeAndAfter {

  var process : Process = null
  var outputWriter : PrintWriter = null
  var inputReader : BufferedReader = null
  var errorReader : BufferedReader = null

  def executeQuery(cmd: String, timeout : Long = 15000) : String = {
    println("Executing " + cmd)
    outputWriter.write(cmd + "\n")
    outputWriter.flush()
    waitForQuery(timeout)
  }

  protected def waitForQuery(timeout: Long) : String = {
    if (waitForOutput(errorReader, "Time taken", timeout)) {
      Thread.sleep(500)
      return readOutput()
    } else {
      assert(false)
      return null
    }
  }

  protected def waitForOutput(reader: BufferedReader, str: String, timeout: Long) : Boolean = {
    val startTime = System.currentTimeMillis
    var out = ""
    while (!out.contains(str) && (System.currentTimeMillis) < (startTime + timeout)) {
      out += readFrom(reader)
    }
    out.contains(str)
  }

  protected def readOutput() : String = {
    val output = readFrom(inputReader)
    // Remove GC Messages
    val filteredOutput = output.lines.filterNot(x => x.contains("[GC") || x.contains("[Full GC"))
      .mkString("\n")
    filteredOutput 
  }

  protected def readFrom(reader: BufferedReader) : String = {
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