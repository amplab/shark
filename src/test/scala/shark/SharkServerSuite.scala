package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfter, FunSuite}

abstract class SharkServerSuite extends SharkCliSuite {

  var serverProcess : Process = null
  var serverInputReader : BufferedReader = null
  var serverErrorReader : BufferedReader = null

  before {
    val serverPb = new ProcessBuilder("./bin/shark", "--service", "sharkserver")
    val serverEnv = serverPb.environment()
    serverProcess = serverPb.start()
    serverInputReader = new BufferedReader(new InputStreamReader(serverProcess.getInputStream))
    serverErrorReader = new BufferedReader(new InputStreamReader(serverProcess.getErrorStream))
    Thread.sleep(5000)

    val clientPb = new ProcessBuilder("./bin/shark", "-h", "localhost")
    process = clientPb.start()
    outputWriter = new PrintWriter(process.getOutputStream, true)
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, ">", 25000)
  }

  after {
    process.destroy()
    process.waitFor()
    serverProcess.destroy()
    serverProcess.waitFor()
  }

  override def waitForQuery(timeout: Long) : String = {
    if (waitForOutput(serverErrorReader, "OK", timeout)) {
      Thread.sleep(1000)
      return readOutput()
    } else {
      assert(false)
      return null
    }
  }

}