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
import java.net.URI
import java.security.SecureClassLoader
import java.util.Arrays
import javax.tools.SimpleJavaFileObject
import javax.tools.JavaFileObject.Kind
import javax.tools.ForwardingJavaFileManager
import javax.tools.JavaFileManager.Location
import javax.tools.Diagnostic
import javax.tools.DiagnosticCollector
import javax.tools.FileObject
import javax.tools.ForwardingJavaFileManager
import javax.tools.StandardJavaFileManager
import javax.tools.JavaFileObject
import javax.tools.JavaCompiler
import javax.tools.ToolProvider

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc

import scala.collection.mutable.LinkedHashSet
import scala.collection.JavaConversions._

import shark.execution.cg.node.ExprNodeCodeGen
import shark.LogHelper

class Definition(
  var defType : Class[_],
  var defName : String,
  var createObjectInDefinition : Boolean,
  var isFinal : Boolean = false,
  var isStatic : Boolean = false,
  var initString : String = null) {
  final override def hashCode = 12 * defName.hashCode + 17 * defType.hashCode

  final override def equals(other : Any) = {
    val that = other.asInstanceOf[Definition]
    if (that == null) false
    else defName == that.defName && defType == that.defType
  }
}

class CodeGenManager(var packageName : String, var className : String, val desc : ExprNodeDesc) extends LogHelper {
  val codegen = new CGContext().create(desc)
  var outputOI : ObjectInspector = null

  def getOutputOI(inputOI : ObjectInspector) : ObjectInspector = {
    if (outputOI == null) {
      outputOI = codegen.initialize(inputOI)
    }

    outputOI
  }

  def evaluator() = evaluateClass(packageName + "." + className, getCode(codegen, packageName, className)).newInstance()

  /*
   * Generating the java source code, the source skeleton as follow: 
   * 
   * package org.apache.hadoop.hive.ql.exec.bytecode.example;
   * 
   * import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
   * import shark.execution.cg.IEvaluate;
   * 
   * public class GENc7de3240_2a42_4354_b310_8803cc686c2e extends IEvaluate {
   *   /*properties definition*/
   * 
   *   @Override
   *   public void init(ObjectInspector oi) {
   *     /* initializations */
   *   }
   * 
   *   @Override
   *   public Object evaluate(Object data) {
   *     /* evaluating */
   *   }
   * }
   */
  protected def getCode(codegen : ExprNodeCodeGen[_], packageName : String, className : String) : String = {
    var code = new StringBuffer
    // 1. define the package name
    code.append("package %s;\n".format(packageName))

    // 2. imports
    code.append("import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;\n")
    code.append("import shark.execution.cg.IEvaluate;\n")
    code.append("\n")

    // 3. class definition
    code.append("public class %s extends IEvaluate {\n".format(className))
    
    // 4. fields
    var fun_def = (d : Definition) =>
      if (d.createObjectInDefinition) 
        "private %s%s %s %s %s;".format(
            if (d.isFinal) "final " else "",
            if (d.isStatic) "static " else "",
            d.defType.getCanonicalName(),
            d.defName,
            if (d.initString == null) ("= new " + d.defType.getCanonicalName() + "()") else d.initString
        )
      else
        "private %s%s %s = null;".format(
            if (d.isStatic) "static " else "",
            d.defType.getCanonicalName(),
            d.defName
         )
    codegen.getContext().getDefinitions().foreach((x : Definition) => code.append(fun_def(x) + "\n"))

    // 5. init function implementation
    code.append("  @Override\n")
    code.append("  public void init(ObjectInspector oi) {\n")

    codegen.codeInit().foreach(code.append(_).append("\n"))

    code.append("  }\n\n") // end of the init function

    // 6. evaluate function implementation
    code.append("@Override\n")
    code.append("  public Object evaluate(Object data) {\n")
    if (codegen.evaluationType() == EvaluationType.CONSTANT) {
      code.append("  return " + codegen.resultVariableName() + ";\n")
    } else if (codegen.evaluationType() == EvaluationType.GET) {
      code.append(codegen.codeEvaluate())
      code.append("  return " + codegen.resultVariableName() + ";\n")
    } else if (codegen.evaluationType() == EvaluationType.SET) {
      code.append(codegen.codeEvaluate())
      var validateCheck = codegen.cgValidateCheck()
      if (null != validateCheck) {
        code.append("if(" + validateCheck + ") {\n")
        code.append("  return " + codegen.resultVariableName() + ";\n")
        code.append("} else {\n")
        code.append("  return null;\n")
        code.append("}\n")
      } else {
        code.append("  return " + codegen.resultVariableName() + ";\n")
      }
    }
    code.append("  }\n\n") // end of the evaluate function
    code.append("}\n") // end of the class

    // returns the java source code
    code.toString()
  }

  def evaluateClass(classFullName : String, code : String) = {
    val compiler = ToolProvider.getSystemJavaCompiler()
    val dc = new DiagnosticCollector[JavaFileObject]()
    var fileManager = new ClassFileManager(compiler
      .getStandardFileManager(null, null, null))
    var file : JavaFileObject = new JavaSourceObject(classFullName, code)
    var compilationUnits = Arrays.asList(file)
    var task = compiler.getTask(null, fileManager, dc, null, null, compilationUnits)
    var result = task.call()

    for (d <- dc.getDiagnostics()) {
      logError(d.getSource().toString())
    }

    outStream().println("Generated Code:")
    outStream().println(code)

    if (result) {
      outStream().println("Success in Compiling: " + classFullName)
      // TODO need to think about how to share the class among the cluster
      fileManager.getClassLoader(null).loadClass(classFullName).asInstanceOf[Class[IEvaluate]]
    } else {
      var err = "Error in compiling class: " + classFullName
      logError(err)
      throw new Exception(err)
    }
  }
}

private class JavaClassObject(name : String) extends 
  SimpleJavaFileObject(URI.create("string:///" + name.replace('.', '/') + Kind.CLASS.extension), Kind.CLASS) {
  private[this] val bos : ByteArrayOutputStream = new ByteArrayOutputStream();

  def getBytes() = bos.toByteArray()
  override def openOutputStream() : OutputStream = bos
}

private class JavaSourceObject(className : String, val contents : String) extends 
  SimpleJavaFileObject(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE) {
  override def getCharContent(ignoreEncodingErrors : Boolean) : CharSequence = contents
}

private class ClassFileManager(standardManager : StandardJavaFileManager) extends 
  ForwardingJavaFileManager[StandardJavaFileManager](standardManager) {
  var jclassObject : JavaClassObject = null

  override def getClassLoader(location : Location) : ClassLoader = new SecureClassLoader() {
    override def findClass(name : String) = {
      var b : Array[Byte] = jclassObject.getBytes()
      super.defineClass(name, jclassObject
        .getBytes(), 0, b.length)
    }
  }

  override def getJavaFileForOutput(location : Location,
                                    className : String, kind : Kind, sibling : FileObject) = {
    this.jclassObject = new JavaClassObject(className)
    this.jclassObject
  }
}

