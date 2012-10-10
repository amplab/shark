package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfter, FunSuite}

abstract class SharkShellCliSuite extends SharkCliSuite {

  before {
    val pb = new ProcessBuilder("./bin/shark")
    process = pb.start()
    outputWriter = new PrintWriter(process.getOutputStream, true)
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "shark>", 25000)
  }

  after {
    process.destroy()
    process.waitFor()
  }

}

