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

import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.broadcast.Broadcast

import shark.{ SharkConfVars, LogHelper }
import shark.execution.cg.OperatorExecutor
import shark.execution.cg.CompilationContext
import shark.execution.cg.row.CGOIField
import shark.execution.cg.row.CGOI
import shark.execution.cg.row.CGOIStruct
import shark.execution.cg.row.CGRow
import shark.execution.cg.row.CGStruct
import shark.execution.cg.row.CGField
import shark.execution.cg.OperatorClassLoader

trait CGObjectOperator extends LogHelper {
  self: Operator[HiveDesc] =>

  // schema of the output / input tuples (table)
  @transient var cgrow: CGStruct = _
  @transient var cginputrows: Array[CGStruct] = _
  
  @transient protected var cgexec: OperatorExecutor = _
  @transient protected var soi: StructObjectInspector = _
  @transient var classloaderBD: Broadcast[OperatorClassLoader] = _
  
  @transient lazy val outputObjectInspector = soi
  
  @BeanProperty var useCG: Boolean = true
  @BeanProperty var operatorClassName: String = _
  @BeanProperty var soiClassName: String = _
  
  protected def useCGObjectInspector = false
  
  def broadcastClassloader(classloaderBD: Broadcast[OperatorClassLoader]) {
    self.parentOperators.map(_.broadcastClassloader(classloaderBD))
    this.classloaderBD = classloaderBD
  }
  
  def cgOnMaster(cc: CompilationContext) {
    soi   = self.createOutputObjectInspector().asInstanceOf[StructObjectInspector]
    useCG = useCG && SharkConfVars.getBoolVar(Operator.hconf, SharkConfVars.QUERY_CG)
    
    if (!useCG) {
      // if not using cg
      return
    }
    
    val compileUnits = new ArrayBuffer[(String, String)]()
    try {
      // collect all of the input table schema
      cginputrows = analyzeInputs()
      soi = createOutputStructObjectInspector(compileUnits, cc)
      operatorClassName = operatorCode(compileUnits, cc)
    } catch {
      case e: Throwable => {
        logWarning("Exception threw, will switch to Hive Evaluator, Msg:" + e.getMessage())
        logInfo("Exception Detail:", e)
        
        cc.remove(compileUnits.toList)
        useCG = false
      }
    }
  }
  
  private def analyzeInputs() = {
    self.parentOperators.toArray.map(op => {
      if(op.cgrow != null) {
        op.cgrow
      } else {
        CGField.create(op.outputObjectInspector)
      }
    })
  }
  
  private def operatorCode(compileUnits: ArrayBuffer[(String, String)], cc: CompilationContext) = {
    var operator = createCGOperator()
    
    if(operator == null) {
      // if not CGOperator created
      useCG = false
      null
    } else {
      val opClassName = operator.fullClassName
      // Put the CG Operator Compilation Unit into context, all of the CG operators 
      // will be compiled together later on
      val tuple = time(()=>List((opClassName, CGOperator.generate(operator))), "Generating Operator")
      
      compileUnits ++= tuple
      
      cc.add(tuple)

      opClassName
    }
  }
  
  private def createOutputStructObjectInspector(compileUnits: ArrayBuffer[(String, String)], 
    cc: CompilationContext) = {
    cgrow = createOutputRow()
    
	val oi: StructObjectInspector = if(cgrow != null) {
	  if(useCGObjectInspector) {
	    // if current operator will create the new schema as output
	    var oiStruct = CGOIField.create(cgrow).asInstanceOf[CGOIStruct]
	    soiClassName = oiStruct.fullClassName
	  
	    // compile the row and its OI object, which will be used in initializing the child Operators
	    val objrow = time(()=>List(
	      (cgrow.fullClassName, CGRow.generate(cgrow, true)), 
	      (soiClassName, CGOI.generateOI(oiStruct, true))), "Generate CGOI/Row")
	    time(()=>cc.compile(objrow), "Compiling CGRow/OI")
	    compileUnits ++= objrow
	  
	    // override the existed output object inspector (StructObjectInspector)
	    instance[StructObjectInspector](oiStruct.fullClassName)
	  } else {
	    import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
        import scala.collection.JavaConversions._
        import shark.execution.cg.row.TypeUtil
        ObjectInspectorFactory.getStandardStructObjectInspector(
          asJavaList(cgrow.fields.map(_.oiName)), 
          asJavaList(cgrow.fields.map(f => TypeUtil.getDataType(f.typeInfo).oi)))
	  }
	} else {
	  soi
	}
	
	oi
  }
  
  private def time[T](func: () => T, msg: String) = {
    val s = System.currentTimeMillis()
    val result = func()
    
    val e = System.currentTimeMillis()
    logWarning("%s takes %s ms".format(msg, e - s))
    
    result
  }
  
  // every code generation operator have to call cgOnSlave() within the initializeOnSlave explicitly
  // There is no real code gen in slave, but instantiating the CGed object.
  protected def cgOnSlave() {
    if(useCG) {
      cgexec = instance[OperatorExecutor](operatorClassName)
      cgexec.init(objectInspectors.toArray)
    }
  }

  // every operator need to codegen its dynamic execution have to override this function
  protected def createCGOperator(): CGOperator = null
  protected def createOutputRow(): CGStruct = null
  
  private def instance[T](clz: String, args: Array[Object]): T = {
    val cl = Thread.currentThread().getContextClassLoader()
    cl.loadClass(clz).getDeclaredConstructors()(0).newInstance(args: _*).asInstanceOf[T]
  }
  
  private def instance[T](clz: String): T = { 
    val cl = Thread.currentThread().getContextClassLoader()
    cl.loadClass(clz).newInstance().asInstanceOf[T]
  }
}