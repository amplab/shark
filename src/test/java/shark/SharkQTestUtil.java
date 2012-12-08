package shark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Replaces Hive's QTestUtil class by using the SharkDriver instead of Hive's
 * Driver. Also changes the way comparison is done by forcing a sort and
 * truncating floating point numbers.
 */
public class SharkQTestUtil extends QTestUtil {

  private static Method maskPatternsMethod;

  static {
    try {
      maskPatternsMethod = QTestUtil.class.getDeclaredMethod("maskPatterns",
          String[].class, String.class);
      maskPatternsMethod.setAccessible(true);
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
  }

  private SharkCliDriver cliDrv;

  public SharkQTestUtil(String outDir, String logDir) throws Exception {
    super(outDir, logDir);
  }

  public SharkQTestUtil(String outDir, String logDir, boolean miniMr,
      String hadoopVer) throws Exception {
    super(outDir, logDir, miniMr, hadoopVer);
  }

  @Override
  public void cliInit(String tname, boolean recreate) throws Exception {
    SharkConfVars.setVar(conf, SharkConfVars.EXPLAIN_MODE(), "hive");

    if (recreate) {
      cleanUp();
      createSources();
    }

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
      "org.apache.hadoop.hive.ql.security.DummyAuthenticator");

    CliSessionState ss = new CliSessionState(conf);
    assert(ss != null);
    ss.in = System.in;

    File qf = new File(outDir, tname);
    File logDirF = new File(logDir);
    if (!logDirF.exists() && !logDirF.mkdirs()) {
      throw new IOException("Could not create directory " + logDir);
    }
    File outf = new File(logDirF, qf.getName().concat(".out"));

    FileOutputStream fo = new FileOutputStream(outf);
    ss.out = new PrintStream(fo, true, "UTF-8");
    ss.err = ss.out;
    ss.setIsSilent(true);
    SessionState oldSs = SessionState.get();
    if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
      oldSs.out.close();
    }
    SessionState.start(ss);

    cliDrv = new SharkCliDriver();
    if (tname.equals("init_file.q")) {
      File testInitFile = new File("../data/scripts/test_init_file.sql");
      try {
        ss.initFiles.add(testInitFile.getAbsolutePath());
      } catch (Exception e) {
        System.out.println("Exceptione is =" + e.getMessage());
      }
    }
    cliDrv.processInitFiles(ss);
  }

  @Override
  public int executeClient(String tname) {
    return cliDrv.processLine(getQMap().get(tname));
  }

  @Override
  public int checkCliDriverResults(String tname) throws Exception {
    String[] cmdArray;
    String[] patterns;
    assert(getQMap().containsKey(tname));

    String outFileName = outPath(outDir, tname + ".out");

    patterns = new String[] {
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
        "^Deleted.*",
    };
    maskPatternsMethod.invoke(this, patterns, (new File(logDir, tname + ".out")).getPath());

    cmdArray = new String[] {
        "diff", "-a",
        "-I", "PREHOOK",
        "-I", "POSTHOOK"
    };

    // Only keep 5 digits of precision for floating point numbers.
    // Also trim trailing whitespace.
    String truncFloatCmd = "perl -p -e 's/(\\d\\.\\d{5})\\d*/\\1/g;' -e 's/\\s+$/\\n/g'";
    String expectedFile = (new File(logDir, tname + ".out")).getPath();

    String cmdString = "\""
        + StringUtils.join(cmdArray, "\" \"") + "\" "
        + "<(sort " + expectedFile + " | " + truncFloatCmd + ") "
        + "<(sort " + outFileName + " | " + truncFloatCmd + ")";
    System.out.println("Comparing: " + expectedFile + " " + outFileName);
    System.out.println(cmdString);

    //System.out.println(org.apache.commons.lang.StringUtils.join(cmdArray, ' '));
    String[] bashCmdArray = new String[3];
    bashCmdArray[0] = "bash";
    bashCmdArray[1] = "-c";
    bashCmdArray[2] = cmdString;
    Process executor = Runtime.getRuntime().exec(bashCmdArray);

    StreamPrinter outPrinter = new StreamPrinter(
        executor.getInputStream(), null, SessionState.getConsole().getChildOutStream());
    StreamPrinter errPrinter = new StreamPrinter(
        executor.getErrorStream(), null, SessionState.getConsole().getChildErrStream());

    outPrinter.start();
    errPrinter.start();

    int exitVal = executor.waitFor();

    if (exitVal != 0 && overWrite) {
      System.out.println("Overwriting results");
      cmdArray = new String[3];
      cmdArray[0] = "cp";
      cmdArray[1] = (new File(logDir, tname + ".out")).getPath();
      cmdArray[2] = outFileName;
      executor = Runtime.getRuntime().exec(cmdArray);
      exitVal = executor.waitFor();
    }

    return exitVal;
  }

}
