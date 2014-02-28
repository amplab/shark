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

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import shark.SharkConfVars
import shark.execution.cg.OperatorExecutor
import shark.execution.cg.CompilationContext
import shark.execution.cg.row.CGOIField
import shark.execution.cg.row.CGOI
import shark.execution.cg.row.CGOIStruct
import shark.execution.cg.row.CGRow
import shark.execution.cg.row.CGStruct
import shark.execution.cg.row.CGField

trait CGObjectOperator {
  self: Operator[HiveDesc] =>

  // schema of the output / input tuples (table)
  @transient var cgrow: CGStruct = _
  @transient var cginputrows: Array[CGStruct] = _
  
  @transient protected var cgexec: OperatorExecutor = _
  @transient protected var soi: StructObjectInspector = _
  
  @transient lazy val outputObjectInspector = soi
  
  @BeanProperty var useCG: Boolean = true
  @BeanProperty var operatorClassName: String = _
  @BeanProperty var soiClassName: String = _
  
  def cgOnMaster(cc: CompilationContext) {
    soi   = self.createOutputObjectInspector().asInstanceOf[StructObjectInspector]
    useCG = useCG && SharkConfVars.getBoolVar(Operator.hconf, SharkConfVars.QUERY_CG)
    
    if (!useCG) {
      // if not using cg
      return
    }
    
    var operator = createCGOperator()
    
    if(operator == null) {
      // if not CGOperator created
      useCG = false
      return
    }

    // collect all of the input table schema
    cginputrows = self.parentOperators.toArray.map(op => {
      if(op.cgrow != null) {
        op.cgrow
      } else {
        CGField.create(op.outputObjectInspector)
      }
    })
    
    cgrow = createOutputRow()
    if(cgrow != null) {
      // if current operator will create the new schema as output
      var oiStruct = CGOIField.create(cgrow).asInstanceOf[CGOIStruct]
      soiClassName      = oiStruct.fullClassName
      
      // compile the row and its OI object, which will be used in initializing the child Operators 
      cc.compile(List(
        (cgrow.fullClassName, CGRow.generate(cgrow, true)), 
        (soiClassName, CGOI.generateOI(oiStruct, true))))
    
      // override the existed output object inspector (StructObjectInspector)
      soi = instance[StructObjectInspector](oiStruct.fullClassName)
    }
    
    operatorClassName = operator.fullClassName
    // Put the CG Operator Compilation Unit into context, all of the CG operators 
    // will be compiled together later on  
    cc.add(List((operatorClassName, CGOperator.generate(operator))))
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
  
  private def instance[T](clz: String, args: Array[Object]): T = 
    Thread.currentThread().getContextClassLoader().loadClass(clz)
      .getDeclaredConstructors()(0).newInstance(args: _*).asInstanceOf[T]
  
  private def instance[T](clz: String): T = 
    Thread.currentThread().getContextClassLoader()
      .loadClass(clz).newInstance().asInstanceOf[T]
}