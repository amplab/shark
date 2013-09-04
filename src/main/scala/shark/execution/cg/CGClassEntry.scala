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

package shark.execution.cg

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.net.URI
import java.util.Arrays
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.security.SecureClassLoader
import javax.tools.SimpleJavaFileObject
import javax.tools.JavaFileObject.Kind
import javax.tools.ForwardingJavaFileManager
import javax.tools.JavaFileManager.Location
import javax.tools.JavaFileManager
import javax.tools.Diagnostic
import javax.tools.DiagnosticCollector
import javax.tools.DiagnosticListener
import javax.tools.FileObject
import javax.tools.ForwardingJavaFileManager
import javax.tools.StandardJavaFileManager
import javax.tools.JavaFileObject
import javax.tools.JavaCompiler
import javax.tools.ToolProvider
import scala.collection.mutable.LinkedHashSet
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,SynchronizedMap}
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import shark.LogHelper
import shark.execution.cg.node.CodeNode
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.io.FileUtils


/**
 * Get the CGExecutor instance from ExprNodeDesc:
 */
class CGClassEntry(val desc: ExprNodeDesc)
  extends LogHelper {

  private var clazz: Class[IEvaluate] = _
  private var outputOI: ObjectInspector = _
  private var initialized = false

  /**
   * initialize the new Evaluator instance with input object inspector
   */
  def initialize(rowInspector: ObjectInspector) = {
    if (!initialized) {
      synchronized {
        if (!initialized) { // double check
          try {
            logWarning("trying codegen for %s".format(desc.getExprString()))
            var node = new CodeNode(
              CGClassEntry.PACKANGE_NAME,
              CGClassEntry.getRandomClassName(),
              desc)
            if (node.fold(rowInspector)) {
              // could handle the ObjectInspector
              outputOI = node.getOutputInspector()

              if (outputOI != null) {
                if (node.constantNull()) {
                  logInfo("Constant Null for %s".format(desc.getExprString()))
                  clazz = NullEvaluatorClass()
                } else {
                  clazz = createClass(node)
                }
              }
            }
          } catch {
            // if anything wrong with the code gen, then switch off the code gen
            case ioe: Throwable => {
              logWarning("failed in codegen for %s".format(desc.getExprString()))
              outputOI = null
              clazz = null
            }
          }
          initialized = true
        }
      }
    }

    evaluator(rowInspector)
  }

  def getOutputOI() = outputOI

  protected def evaluator(rowInspector: ObjectInspector): IEvaluate = {
    if (outputOI == null || clazz == null)
      null
    else {
      var obj = clazz.newInstance()
      obj.init(rowInspector)

      obj
    }
  }

  protected def createClass(node: CodeNode): Class[IEvaluate] = {
    var code = node.cgEvaluate()
    logInfo("%s==>\n%s".format(node.desc.getExprString(), code))
    var clz = new JavaCompilerHelper().compile(node.fullClassName, code)

    clz.asInstanceOf[Class[IEvaluate]]
  }
}

object CGClassEntry extends LogHelper {
  private val CACHE_SIZE = 50 // TODO make it configurable
  private val PACKANGE_NAME: String = "org.apache.hadoop.hive.ql.exec.cg"
  private val tuples = new java.util.LinkedList[(ExprNodeGenericFuncDesc, CGClassEntry)]()

  private def getRandomClassName() = "GEN" + UUID.randomUUID().toString().replaceAll("\\-", "_")

  def apply(desc: ExprNodeGenericFuncDesc): CGClassEntry = {
    logInfo("LRU tuples size:%s".format(tuples.size()))
    
    var continue = true
    var tuple : (ExprNodeGenericFuncDesc, CGClassEntry) = null
    
    synchronized {
      var ite = tuples.iterator()
      // Simple LRU list 
      while(ite.hasNext && continue) {
        tuple = ite.next()
        if(tuple._1.isSame(desc)) {
          ite.remove() // 0. pick & remove
          logInfo("LRU cache hit [%s]".format(desc))
          continue = false
        }
      }
      
      if (continue) {
        // not found
        tuple = (desc, new CGClassEntry(desc)) // 0. or create
        logInfo("LRU cache miss [%s]".format(desc))
      }
      
      tuples.addFirst(tuple) // 1. put into the head of the list
  
      // 2. if cache size is greater than threshold, remove the eldest tuple
      if (tuples.length > CACHE_SIZE) tuples.removeLast()
    }
    tuple._2
  }
}

/**
 * helper to compile the java source code
 */
class JavaCompilerHelper extends LogHelper with DiagnosticListener[JavaFileObject] {
  private[this] val classFileManager = new SingleClassFileManager()

  override def report(d: Diagnostic[_ <: JavaFileObject]) {
    logError("Line:%s Msg:%s Source:%s".
      format(d.getLineNumber(), d.getMessage(Locale.US), d.getSource()))
  }

  /**
   * get the class path paired with "-cp" from the context thread class loader,
   */
  private def compileOptions() = {
    classPathOption(Thread.currentThread().getContextClassLoader())
    // TODO put more compiling options here.
  }

  /**
   * get the class path paired with "-cp" from the context thread class loader,
   */
  private def classPathOption(cl: ClassLoader): List[String] = {
    // TODO ATTENTION!! 
    // Work around solution to make the JavaCompiler workable under sbt unit test!
    // The class loader of javax.tools.JavaCompiler is sun.misc.Launcher.AppClassLoader, which 
    // means, the dependencies(jars) of the runtime compiled java source, have to be specified
    // via the -cp option when the JVM process starts. That's quite nature for shark/spark, cause 
    // they do place the full class path option (-cp) for the spawned jvm processes; but in 
    // sbt, the classpath(-cp) option is "sbt-launch.jar", specified within file sbt/sbt.
    // Fortunately, the JavaCompiler API accepts compiling options, hence we can pass in the 
    // class path(-cp) by retrieving all of dependent jars from the TheadContextClassLoader
    //  
    // Another possible solution is to make the sbt unittest runs within a spawned new jvm process,
    // but I haven't figure out how to do it in .sbt build definition.
    if (!java.lang.Boolean.parseBoolean(System.getProperty(JavaCompilerHelper.FOR_UNIT_TEST_WORK_AROUND, "false"))) {
      // if NOT the sbt unit test, then will not create the class path option for JavaCompiler
      return List[String]()
    }
    var classpath = Array[URL]()
    if (cl.isInstanceOf[URLClassLoader]) {
      classpath ++= cl.asInstanceOf[URLClassLoader].getURLs()
    }

    var classes = System.getProperty("java.class.path")
    classpath ++= (classes.split(File.pathSeparator).map(x => new URL("file:" + x)))

    var sb = new StringBuffer()
    classpath.foreach(x => { sb.append(x.getPath()); sb.append(File.pathSeparatorChar) })

    logWarning("Using [-cp], may cause the PermGen OOM!")
    List[String]("-cp", sb.toString())
  }

  /**
   * compile the java source code
   * @param classFullName class name with package name
   * @param code java source code
   * @return class
   * @throws CGAssertRuntimeException if anything wrong in compiling
   */
  def compile(fullClassName: String, code: String) = {
    classFileManager.compile(List((fullClassName, code)), this, compileOptions())
  }
  
  def compile(entries: List[(String, String)]) = {
    classFileManager.compile(entries, this, compileOptions())
  }
  
  def getOperatorClassLoader() = classFileManager.getClassLoader(null).asInstanceOf[OperatorClassLoader]
}

object JavaCompilerHelper {
  val FOR_UNIT_TEST_WORK_AROUND = "for_unit_test_workaround"
  val CG_SOURCE_CODE_PATH = "cg_source_code_path"
}
  
/**
 * Customed class loader, which is single thread "write" in Master node and 
 * multiple "read" in Workers
 * 
 */
class OperatorClassLoader(parent: ClassLoader) 
  extends SecureClassLoader(parent) with LogHelper with KryoSerializable {
  
  def this() = this(Thread.currentThread.getContextClassLoader())
  
  def addClassEntries(ocl: Array[OperatorClassLoader]) {
    ocl.foreach(cl => clazzCache.putAll(cl.clazzCache))
  }
  
  var clazzCache = new HashMap[String, Array[Byte]] with SynchronizedMap[String, Array[Byte]]

  def flushClass(name: String, bytes: Array[Byte]) = {
    clazzCache.getOrElseUpdate(name, bytes)
  }
  
  override def findClass(name: String): Class[_] = clazzCache.get(name) match {
      case Some(b) => defineClass(name, b, 0, b.length)
      case None => throw new ClassNotFoundException(name)
  }

  override def read(kryo: Kryo, input: Input) {
    clazzCache = new HashMap[String, Array[Byte]] with SynchronizedMap[String, Array[Byte]]
    var size = input.read()
    for(i <- 0 until size) {
      var name = input.readString()
      var bytes = new Array[Byte](input.readInt())
      input.readBytes(bytes)
      flushClass(name, bytes)
    }
  }

  override def write(kryo: Kryo, output: Output) {
    output.write(clazzCache.size)
    clazzCache.foreach { case (name, bytes) => {
        output.writeString(name)
        output.writeInt(bytes.length)
        output.writeBytes(bytes, 0, bytes.length)
      }
    }
  }
}

class JavaSourceObject(className: String, val contents: String)
  extends SimpleJavaFileObject(
    URI.create(
      "string:///" + className.replace('.', '/') + Kind.SOURCE.extension),
    Kind.SOURCE) {

  override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = contents
}

private class SingleClassFileManager(
    private val compiler: JavaCompiler = ToolProvider.getSystemJavaCompiler())
  extends ForwardingJavaFileManager[StandardJavaFileManager](
      compiler.getStandardFileManager(null, null, null))
  with LogHelper {

  class JavaClassObject(val name: String)
    extends SimpleJavaFileObject(
      URI.create("string:///" + name.replace('.', '/') + Kind.CLASS.extension),
      Kind.CLASS) {
    var bytes: Array[Byte] = _

    override def openOutputStream(): OutputStream =
      new ByteArrayOutputStream() {
        override def close() {
          super.close()
          bytes = this.toByteArray()
          cl.flushClass(name, bytes)
        }
      }
  }

  private var cl = new OperatorClassLoader(this.getClass().getClassLoader())

  override def getClassLoader(location: Location): ClassLoader = cl

  override def getJavaFileForOutput(location: Location,
                                    className: String, kind: Kind, sibling: FileObject) = {
    new JavaClassObject(className)
  }

  /**
   *  List[(String,String)], the tuple is (className, CodeContent)
   */
  def compile(files: List[(String, String)],
              dl: DiagnosticListener[JavaFileObject],
              options: List[String]) {
    // for debugging the generate source code purpose
    val cgSourcePath = System.getProperty(JavaCompilerHelper.CG_SOURCE_CODE_PATH)
    if (cgSourcePath != null) {
      files.foreach(file => {
        val sourceFilePath = 
          cgSourcePath + File.separator + file._1.replaceAll("\\.", File.separator) + ".java"
        FileUtils.writeStringToFile(new File(sourceFilePath), file._2)
      })
    }
    
    // convert the (className, CodeContent) ==> JavaSourceObject
    var jsObject = files.map(x => new JavaSourceObject(x._1, x._2))

    var result = compiler.getTask(null, this, dl, options, null, jsObject).call()

    if (!result) {
      files.foreach(x => {
        logError("Compiling: " + x._1)
        logError("Generated Code: " + x._2)
      })
      throw new CGAssertRuntimeException("Error in compiling classes")
    }
  }

  def loadClass(fullClassName: String) = cl.loadClass(fullClassName)
}
