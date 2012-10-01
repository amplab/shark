package shark

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.QTestUtil

class SharkQTestUtil(outDir: String, logDir: String) extends QTestUtil(outDir, logDir) {

  val queryMap = getQMap

  var cliDrv: SharkCliDriver = null

  val maskPatternsMethod = this.getClass.getSuperclass.getDeclaredMethod(
    "maskPatterns", classOf[Array[String]], classOf[String])
  maskPatternsMethod.setAccessible(true)

  override def cliInit(tname:String, recreate:Boolean) {
    //HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
    //"org.apache.hadoop.hive.ql.security.DummyAuthenticator")
    
    SharkConfVars.setVar(conf, SharkConfVars.EXPLAIN_MODE, "hive")
    
    val ss = new CliSessionState(conf)
    assert(ss != null)
    ss.in = System.in
    
    val qf = new File(outDir, tname)
    var outf = new File(logDir)
    outf = new File(outf, qf.getName.concat(".out"))
    val fo = new FileOutputStream(outf)
    ss.out = new PrintStream(fo, true, "UTF-8")
    ss.err = ss.out
    ss.setIsSilent(true)
    val oldSs = SessionState.get()
    if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
      oldSs.out.close()
    }
    SessionState.start(ss)
    
    cliDrv = new SharkCliDriver()
    //    if (tname.equals("init_file.q"))
    //      ss.initFiles.add("../data/scripts/test_init_file.sql")
    cliDrv.processInitFiles(ss)
  }

  override def executeClient(tname: String): Int = {
    cliDrv.processLine(queryMap.get(tname))
  }

  override def checkCliDriverResults(tname: String): Int = {
    val outFileName = outPath(outDir, tname + ".out")

    val cmdArray: Array[String] = Array(
      "diff", "-a",
      "-I", "PREHOOK",
      "-I", "POSTHOOK")

    val patterns: Array[String] = Array(
        ".*file:.*",
        ".*pfile:.*",
        ".*hdfs:.*",
        ".*/tmp/.*",
        ".*invalidscheme:.*",
        ".*lastUpdateTime.*",
        ".*lastAccessTime.*",
        ".*lastModifiedTime.*",
        ".*[Oo]wner.*",
        ".*CreateTime.*",
        ".*LastAccessTime.*",
        ".*Location.*",
        ".*LOCATION '.*",
        ".*transient_lastDdlTime.*",
        ".*last_modified_.*",
        ".*java.lang.RuntimeException.*",
        ".*at org.*",
        ".*at sun.*",
        ".*at java.*",
        ".*at junit.*",
        ".*Caused by:.*",
        ".*LOCK_QUERYID:.*",
        ".*LOCK_TIME:.*",
        ".*grantTime.*",
        ".*[.][.][.] [0-9]* more.*",
        ".*job_[0-9]*_[0-9]*.*",
        ".*USING 'java -cp.*",
        "^Deleted.*")

    maskPatternsMethod.invoke(this, patterns, (new File(logDir, tname + ".out")).getPath());

    val cmdString = 
      "\"" +
      StringUtils.join(cmdArray.asInstanceOf[Array[Object]], "\" \"") + "\" " + 
      "<(sort " + (new File(logDir, tname + ".out")).getPath() + ") " + 
      "<(sort " + outFileName + ")"

    println(cmdString)
    val bashCmdArray = Array("bash", "-c", cmdString)
    //println(StringUtils.join(cmdArray.asInstanceOf[Array[Object]], ' '))
  
    val executor = Runtime.getRuntime().exec(bashCmdArray);

    val outPrinter = new StreamPrinter(
        executor.getInputStream(), null, SessionState.getConsole().getChildOutStream())
    val errPrinter = new StreamPrinter(
        executor.getErrorStream(), null, SessionState.getConsole().getChildErrStream())

    outPrinter.start();
    errPrinter.start();

    val exitVal = executor.waitFor();

    exitVal
  }

}
