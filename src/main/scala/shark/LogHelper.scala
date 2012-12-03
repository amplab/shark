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
trait LogHelper extends spark.Logging {

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
