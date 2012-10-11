package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SharkServerSuite extends FunSuite with BeforeAndAfter with CliTestToolkit {

  var serverProcess : Process = null
  var serverInputReader : BufferedReader = null
  var serverErrorReader : BufferedReader = null

  before {
    val serverPb = new ProcessBuilder("./bin/shark", "--service", "sharkserver")
    val serverEnv = serverPb.environment()
    serverEnv.put("SHARK_LAUNCH_WITH_JAVA", "1")
    serverProcess = serverPb.start()
    serverInputReader = new BufferedReader(new InputStreamReader(serverProcess.getInputStream))
    serverErrorReader = new BufferedReader(new InputStreamReader(serverProcess.getErrorStream))
    Thread.sleep(5000)

    val clientPb = new ProcessBuilder("./bin/shark", "-h", "localhost")
    process = clientPb.start()
    outputWriter = new PrintWriter(process.getOutputStream, true)
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "shark>")
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

  test("Simple Query against Shark Server") {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    executeQuery("drop table if exists test;");
    executeQuery("create table test(key int, val string);")
    executeQuery("load data local inpath '" + dataFilePath+ "' overwrite into table test;")
    val out = executeQuery("select * from test where key = 407;")
    assert(out.contains("val_407"))
    //executeQuery("exit;")
  }

}