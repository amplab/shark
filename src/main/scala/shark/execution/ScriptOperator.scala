package shark.execution

import java.io.{File, InputStream}
import java.util.{Arrays, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{ScriptOperator => HiveScriptOperator}
import org.apache.hadoop.hive.ql.exec.{RecordReader, RecordWriter, ScriptOperatorHelper}
import org.apache.hadoop.hive.ql.plan.ScriptDesc
import org.apache.hadoop.hive.serde2.{Serializer, Deserializer}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.{BytesWritable, Writable}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.BeanProperty

import spark.{OneToOneDependency, RDD, SparkEnv, Split}


/**
 * An operator that runs an external script. We use a customized version of
 * PipedRDD (CustomPipedRdd) to call the external script.
 *
 * Example: select transform(key) using 'cat' as cola from src;
 */
class ScriptOperator extends UnaryOperator[HiveScriptOperator] {

  @BeanProperty var localHiveOp: HiveScriptOperator = _
  @BeanProperty var localHconf: HiveConf = _
  @BeanProperty var alias: String = _

  @transient var scriptInputSerializer: Serializer = _
  @transient var scriptOutputDeserializer: Deserializer = _

  /**
   * Override execute. (Don't deal with preprocessRdd, postprocessRdd here.)
   */
  override def execute(): RDD[_] = {
    // First run parent.
    val inputRdd = executeParents().head._2

    val op = OperatorSerializationWrapper(this)
    // Serialize the data so it is recognizable by the script.
    val rdd = inputRdd.mapPartitions { part =>
      op.initializeOnSlave()
      op.serializeForScript(part)
    }

    val outRecordReaderClass = hiveOp.getConf().getOutRecordReaderClass()
    val inRecordWriterClass = hiveOp.getConf().getInRecordWriterClass()

    logInfo("Using %s and %s".format(outRecordReaderClass, inRecordWriterClass))

    // Create a piped rdd (this executes the script).
    val (command, envs) = getCommandAndEnvs()
    val piped = new CustomPipedRdd(
        rdd,
        command,
        envs,
        outRecordReaderClass,
        inRecordWriterClass,
        XmlSerializer.serialize(hconf),
        hiveOp.getConf().getScriptOutputInfo().getProperties())

    // Deserialize the output from script back to what Hive understands.
    piped.mapPartitions { part =>
      op.initializeOnSlave()
      op.deserializeFromScript(part)
    }
  }

  override def initializeOnMaster() {
    localHiveOp = hiveOp
    localHconf = super.hconf
    // Set parent to null so we won't serialize the entire query plan.
    hiveOp.setParentOperators(null)
    hiveOp.setChildOperators(null)
    hiveOp.setInputObjInspectors(null)
  }

  override def initializeOnSlave() {
    scriptOutputDeserializer = localHiveOp.getConf().getScriptOutputInfo()
        .getDeserializerClass().newInstance()
    scriptOutputDeserializer.initialize(localHconf, localHiveOp.getConf()
        .getScriptOutputInfo().getProperties())

    scriptInputSerializer = localHiveOp.getConf().getScriptInputInfo().getDeserializerClass()
        .newInstance().asInstanceOf[Serializer]
    scriptInputSerializer.initialize(
        localHconf, localHiveOp.getConf().getScriptInputInfo().getProperties())
  }

  /**
   * Generate the command and the environmental variables for running the
   * script. This is called on the master.
   */
  def getCommandAndEnvs(): (Array[String], Map[String, String]) = {

    val scriptOpHelper = new ScriptOperatorHelper(new HiveScriptOperator)
    alias = scriptOpHelper.getAlias

    val cmdArgs = HiveScriptOperator.splitArgs(hiveOp.getConf().getScriptCmd())
    val prog = cmdArgs(0)
    val currentDir = new File(".").getAbsoluteFile()

    if (!(new File(prog)).isAbsolute()) {
      val finder = scriptOpHelper.newPathFinderInstance("PATH")
      finder.prependPathComponent(currentDir.toString())
      var f = finder.getAbsolutePath(prog)
      if (f != null) {
        cmdArgs(0) = f.getAbsolutePath()
      }
    }

    val wrappedCmdArgs = addWrapper(cmdArgs)
    logInfo("Executing " + wrappedCmdArgs.toSeq)
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
    val idEnvVarVal = hiveOp.getOperatorId()
    envs.put(scriptOpHelper.safeEnvVarName(idEnvVarName), idEnvVarVal)

    (wrappedCmdArgs, Map.empty ++ envs)
  }

  override def processPartition(split: Split, iter: Iterator[_]): Iterator[_] = {
    throw new Exception("UnionOperator.processPartition() should've never been called.")
  }

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

  def serializeForScript[T](iter: Iterator[T]): Iterator[Writable] = {
    iter.map { row =>
      scriptInputSerializer.serialize(row, objectInspector)
    }
  }

  def deserializeFromScript(iter: Iterator[Writable]): Iterator[_] = {
    iter.map { row =>
      scriptOutputDeserializer.deserialize(row)
    }
  }
}


/**
 * An RDD that pipes the contents of each parent partition through an external
 * command and returns the output as a collection. We cannot use Spark's
 * PipedRDD because it only supports String inputs/outputs. We implement our own
 * customized version to support Hive's RecordReader and RecordWriter.
 *
 * TODO: We don't actually need a new RDD. We can merge this into the call to
 * mapPartitions of the previous RDD in ScriptOperator.
 */
class CustomPipedRdd(
    parent: RDD[Writable],
    command: Seq[String],
    envVars: Map[String, String],
    recordReaderClass: Class[_ <: RecordReader],
    recordWriterClass: Class[_ <: RecordWriter],
    hconfSerialized: Array[Byte], // HiveConf is not Java serializable.
    recordReaderProperties: java.util.Properties)
  extends RDD[Writable](parent.context) {

  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override def compute(split: Split): Iterator[Writable] = {
    val workingDir = System.getProperty("user.dir")
    val newCmd = command.map { arg =>
      if (new File(workingDir + "/" + arg).exists()) "./" + arg else arg
    }
    val pb = new ProcessBuilder(newCmd)
    pb.directory(new File(workingDir))
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case(variable, value) => currentEnvVars.put(variable, value) }

    val proc = pb.start()
    val env = SparkEnv.get

    val hconf = XmlSerializer.deserialize[HiveConf](hconfSerialized)

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for " + command) {
      override def run() {
        for(line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + command) {
      override def run() {
        SparkEnv.set(env)
        val recordWriter = recordWriterClass.newInstance
        recordWriter.initialize(proc.getOutputStream, hconf)
        for(elem <- parent.iterator(split)) {
          recordWriter.write(elem)
        }
        recordWriter.close()
      }
    }.start()

    // Return an iterator that reads outputs from RecordReader. Use our own
    // BinaryRecordReader if necessary because Hive's has a bug (see below).
    val recordReader =
      if (recordReaderClass == classOf[org.apache.hadoop.hive.ql.exec.BinaryRecordReader]) {
        new CustomBinaryRecordReader
      } else {
        recordReaderClass.newInstance
      }
    recordReader.initialize(proc.getInputStream, hconf, recordReaderProperties)
    new RecordReaderIterator(recordReader)
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

  override def hasNext(): Boolean = {
    _numBytesNextRecord > 0
  }

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
class CustomBinaryRecordReader extends RecordReader {

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
    val recordLength = in.read(bytesWritable.get(), 0, maxRecordLength)
    if (recordLength >= 0) {
      bytesWritable.setSize(recordLength)
    }
    return recordLength;
  }

  override def close() { if (in != null) { in.close() } }
}


