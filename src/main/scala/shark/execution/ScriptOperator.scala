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

package shark.execution

import java.io.{File, InputStream, IOException}
import java.lang.Thread.UncaughtExceptionHandler
import java.util.Properties

import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.BeanProperty

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{RecordReader, RecordWriter}
import org.apache.hadoop.hive.ql.exec.{ScriptOperator => HiveScriptOperator}
import org.apache.hadoop.hive.ql.exec.{ScriptOperatorHelper => HiveScriptOperatorHelper}
import org.apache.hadoop.hive.ql.plan.ScriptDesc
import org.apache.hadoop.hive.serde2.{Serializer, Deserializer}
import org.apache.hadoop.io.{BytesWritable, Writable}

import org.apache.spark.{SparkEnv, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext

import shark.execution.serialization.OperatorSerializationWrapper
import shark.LogHelper


/**
 * An operator that runs an external script.
 *
 * Example: select transform(key) using 'cat' as cola from src;
 */
class ScriptOperator extends UnaryOperator[ScriptDesc] {

  @BeanProperty var localHconf: HiveConf = _
  @BeanProperty var alias: String = _
  @BeanProperty var conf: ScriptDesc = _

  @transient var operatorId: String = _
  @transient var scriptInputSerializer: Serializer = _
  @transient var scriptOutputDeserializer: Deserializer = _

  /**
   * Override execute. (Don't deal with preprocessRdd, postprocessRdd here.)
   */
  override def execute(): RDD[_] = {
    // First run parent.
    val inputRdd = executeParents().head._2

    val op = OperatorSerializationWrapper(this)
    val (command, envs) = getCommandAndEnvs()
    val outRecordReaderClass: Class[_ <: RecordReader] = conf.getOutRecordReaderClass()
    val inRecordWriterClass: Class[_ <: RecordWriter] = conf.getInRecordWriterClass()
    logDebug("Using %s and %s".format(outRecordReaderClass, inRecordWriterClass))

    // Deserialize the output from script back to what Hive understands.
    inputRdd.mapPartitionsWithContext { (context, part) =>
      op.initializeOnSlave()

      // Serialize the data so it is recognizable by the script.
      val iter = op.serializeForScript(part)

      // Rebuild the command to specify paths on each node.
      // For example, if the command is "python test.py data.dat", the following can turn
      // it into "python /path/to/workdir/test.py /path/to/workdir/data.dat".
      val workingDir = System.getProperty("user.dir")
      val newCmd = command.map { arg =>
        val uploadedFile = SparkFiles.get(arg)
        if (new File(uploadedFile).exists()) {
          uploadedFile
        } else {
          arg
        }
      }
      val pb = new ProcessBuilder(newCmd.toSeq)
      pb.directory(new File(workingDir))
      // Add the environmental variables to the process.
      val currentEnvVars = pb.environment()
      envs.foreach { case(variable, value) => currentEnvVars.put(variable, value) }

      val proc = pb.start()
      val hconf = op.localHconf

      // Get the thread local SparkEnv so we can pass it into the new thread.
      val sparkEnv = SparkEnv.get

      // If true, exceptions thrown by child threads will be ignored.
      val allowPartialConsumption = op.allowPartialConsumption

      // Start a thread to print the process's stderr to ours
      val errorReaderThread = new Thread("stderr reader for " + command) {
        override def run() {
          for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
            System.err.println(line)
          }
        }
      }
      errorReaderThread.setUncaughtExceptionHandler(
        new ScriptOperator.ScriptExceptionHandler(allowPartialConsumption, context))
      errorReaderThread.start()

      // Start a thread to feed the process input from our parent's iterator
      val inputWriterThread = new Thread("stdin writer for " + command) {
        override def run() {
          // Set the thread local SparkEnv.
          SparkEnv.set(sparkEnv)
          val recordWriter = inRecordWriterClass.newInstance
          recordWriter.initialize(proc.getOutputStream, op.localHconf)
          for (elem <- iter) {
            recordWriter.write(elem)
          }
          recordWriter.close()
        }
      }
      inputWriterThread.setUncaughtExceptionHandler(
        new ScriptOperator.ScriptExceptionHandler(allowPartialConsumption, context))
      inputWriterThread.start()

      // Return an iterator that reads outputs from RecordReader. Use our own
      // BinaryRecordReader if necessary because Hive's has a bug (see below).
      val recordReader: RecordReader =
        if (outRecordReaderClass == classOf[org.apache.hadoop.hive.ql.exec.BinaryRecordReader]) {
          new ScriptOperator.CustomBinaryRecordReader
        } else {
          outRecordReaderClass.newInstance
        }
      recordReader.initialize(
        proc.getInputStream,
        op.localHconf,
        op.conf.getScriptOutputInfo().getProperties())

      op.deserializeFromScript(new ScriptOperator.RecordReaderIterator(recordReader))
    }
  }

  override def initializeOnMaster() {
    super.initializeOnMaster()
    localHconf = super.hconf
    conf = desc
    
    initializeOnSlave()
  }

  override def outputObjectInspector() = scriptOutputDeserializer.getObjectInspector()
  
  override def initializeOnSlave() {
    scriptOutputDeserializer = conf.getScriptOutputInfo().getDeserializerClass().newInstance()
    scriptOutputDeserializer.initialize(localHconf, conf.getScriptOutputInfo().getProperties())

    scriptInputSerializer = conf.getScriptInputInfo().getDeserializerClass()
      .newInstance().asInstanceOf[Serializer]
    scriptInputSerializer.initialize(localHconf, conf.getScriptInputInfo().getProperties())
  }

  /**
   * Generate the command and the environmental variables for running the
   * script. This is called on the master.
   */
  def getCommandAndEnvs(): (Seq[String], Map[String, String]) = {

    val scriptOpHelper = new HiveScriptOperatorHelper(new HiveScriptOperator)
    alias = scriptOpHelper.getAlias

    val cmdArgs = HiveScriptOperator.splitArgs(conf.getScriptCmd())
    val prog = cmdArgs(0)
    val currentDir = new File(".").getAbsoluteFile()

    if (!(new File(prog)).isAbsolute()) {
      val finder = scriptOpHelper.newPathFinderInstance("PATH")
      finder.prependPathComponent(currentDir.toString())
      val f = finder.getAbsolutePath(prog)
      if (f != null) {
        cmdArgs(0) = f.getAbsolutePath()
      }
    }

    val wrappedCmdArgs = addWrapper(cmdArgs).toSeq
    logInfo("Executing " + wrappedCmdArgs)
    logInfo("tablename=" + hconf.get(HiveConf.ConfVars.HIVETABLENAME.varname))
    logInfo("partname=" + hconf.get(HiveConf.ConfVars.HIVEPARTITIONNAME.varname))
    logInfo("alias=" + alias)

    // Set environmental variables.
    val envs = new java.util.HashMap[String, String]
    scriptOpHelper.addJobConfToEnvironment(hconf, envs)

    envs.put(scriptOpHelper.safeEnvVarName(HiveConf.ConfVars.HIVEALIAS.varname),
      String.valueOf(alias))

    // Create an environment variable that uniquely identifies this script
    // operator
    val idEnvVarName = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESCRIPTIDENVVAR)
    val idEnvVarVal = operatorId
    envs.put(scriptOpHelper.safeEnvVarName(idEnvVarName), idEnvVarVal)

    (wrappedCmdArgs, Map.empty ++ envs)
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException

  /**
   * Wrap the script in a wrapper that allows admins to control.
   */
  def addWrapper(inArgs: Array[String]): Array[String] = {
    val wrapper = HiveConf.getVar(hconf, HiveConf.ConfVars.SCRIPTWRAPPER)
    if (wrapper == null) {
      inArgs
    } else {
      val wrapComponents = HiveScriptOperator.splitArgs(wrapper)
      Array.concat(wrapComponents, inArgs)
    }
  }

  def allowPartialConsumption: Boolean =
    HiveConf.getBoolVar(localHconf, HiveConf.ConfVars.ALLOWPARTIALCONSUMP)

  def serializeForScript[T](iter: Iterator[T]): Iterator[Writable] =
    iter.map { row => scriptInputSerializer.serialize(row, objectInspector) }

  def deserializeFromScript(iter: Iterator[Writable]): Iterator[_] =
    iter.map { row => scriptOutputDeserializer.deserialize(row) }
}

object ScriptOperator {

  /**
   * A general exception handler to attach to child threads used to feed input rows and forward
   * errors to the parent thread during ScriptOperator#execute().
   * If partial query consumption is not allowed (see HiveConf.Confvars.ALLOWPARTIALCONSUMP), then
   * exceptions from child threads are caught by the handler and re-thrown by the parent thread
   * through an on-task-completion callback registered with the Spark TaskContext. The task will be
   * marked "failed" and the exception will be propagated to the master/CLI.
   */
  class ScriptExceptionHandler(allowPartialConsumption: Boolean, context: TaskContext)
    extends UncaughtExceptionHandler
    with LogHelper {

    override def uncaughtException(thread: Thread, throwable: Throwable) {
      throwable match {
        case ioe: IOException => {
          // Check whether the IOException should be re-thrown by the parent thread.
          if (allowPartialConsumption) {
            logWarning("Error while executing script. Ignoring %s"
              .format(ioe.getMessage))
          } else {
            val onCompleteCallback = () => {
              logWarning("Error during script execution. Set %s=true to ignore thrown IOExceptions."
                .format(HiveConf.ConfVars.ALLOWPARTIALCONSUMP.toString))
              throw ioe
            }
            context.synchronized {
              context.addOnCompleteCallback(onCompleteCallback)
            }
          }
        }
        case _ => {
          // Throw any other Exceptions or Errors.
          val onCompleteCallback = () => throw throwable
          context.synchronized {
            context.addOnCompleteCallback(onCompleteCallback)
          }
        }
      }
    }
  }

  /**
   * An iterator that wraps around a Hive RecordReader.
   */
  class RecordReaderIterator(recordReader: RecordReader) extends Iterator[Writable] {

    // This creates a simple circular buffer. We need it because RecordReader
    // doesn't provide a way to test "hasNext()". We implement hasNext() by always
    // prefetching a record from RecordReader and saves it in our circular buffer
    // of size two.
    private val _buffer = Array(recordReader.createRow(), recordReader.createRow())

    // Hopefully this won't overflow... We don't really expect users to pipe
    // billions of records into a script. If it really overflows, we can simply
    // wrap it around.
    private var _index: Int = 0
    private var _numBytesNextRecord = recordReader.next(_buffer(_index))

    override def hasNext(): Boolean = _numBytesNextRecord > 0

    override def next(): Writable = {
      _index += 1
      _numBytesNextRecord = recordReader.next(_buffer(_index % 2))
      _buffer((_index - 1) % 2)
    }
  }

  /**
   * A custom implementation of Hive's BinaryRecordReader. This one fixes a bug
   * in Hive's code.
   *
   * Read from a binary stream and treat each 1000 bytes (configurable via
   * hive.binary.record.max.length) as a record.  The last record before the
   * end of stream can have less than 1000 bytes.
   */
  private class CustomBinaryRecordReader extends RecordReader {

    private var in: InputStream = _
    private var maxRecordLength: Int = _

    override def initialize(inStream: InputStream, conf: Configuration, tbl: Properties) {
      in = inStream
      maxRecordLength = conf.getInt("hive.binary.record.max.length", 1000)
    }

    override def createRow(): Writable = {
      val bytes = new BytesWritable
      bytes.setCapacity(maxRecordLength)
      return bytes
    }

    def next(row: Writable): Int = {
      // The Hive version doesn't read stuff into the passed row ...
      // It simply reuses the last row.
      val bytesWritable = row.asInstanceOf[BytesWritable]
      val recordLength = in.read(bytesWritable.getBytes(), 0, maxRecordLength)
      if (recordLength >= 0) {
        bytesWritable.setSize(recordLength)
      }
      return recordLength
    }

    override def close() { if (in != null) { in.close() } }
  }
}
