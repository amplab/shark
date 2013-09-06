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

import shark.execution.cg.row.CGStruct
import shark.execution.cg.row.CGOIStruct
import shark.execution.cg.row.CGField
import shark.execution.cg.row.CGRow
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import shark.execution.cg.row.CGOIField
import shark.execution.cg.row.CGOI
import scala.collection.mutable.ArrayBuffer
import shark.execution.cg.operator.CGOperator
import scala.reflect.BeanProperty
import scala.annotation.target.field
import scala.reflect.BeanProperty
import shark.execution.Operator
import shark.execution.HiveDesc
import shark.SharkConfVars

trait CGObjectOperator {
  self: Operator[HiveDesc] =>
  
  // for describing the output data
  @transient var row: CGStruct = _
  @transient var cgexec: OperatorExecutor = _
  @transient var soi: StructObjectInspector = _
  
  @BeanProperty var useCG: Boolean = _
  @BeanProperty var operatorClassName: String = _
  @BeanProperty var soiClassName: String = _
  
  def cgOnMaster(cc: CompilationContext) {
    soi   = createOutputOI()
    useCG = SharkConfVars.getBoolVar(Operator.hconf, SharkConfVars.EXPR_CG)
    
    if (useCG) {
      row = CGField.create(soi)
      
      var operator = createCGOperator()
      var oiStruct = CGOIField.create(row).asInstanceOf[CGOIStruct]
      
      operatorClassName = operator.fullClassName()
      soiClassName      = oiStruct.fullClassName()
      
      // compile the row and oi object, which will be used while initiliazeOnMaster() 
      cc.compile(List(
          (row.fullClassName(), CGRow.generate(row, true)), 
          (soiClassName, CGOI.generateOI(oiStruct, true))))
      
      soi = CGBeanUtils.instance[StructObjectInspector](oiStruct.fullClassName)
      
      // Put the CG Operator Compilation Unit into context, all of the CG operators 
      // will be compiled together later on  
      cc.add(this, List((operatorClassName, CGOperator.generate(operator))))
    }
  }
  
  def cgOnSlave() {
    if(useCG) {
      cgexec = CGBeanUtils.instance[OperatorExecutor](operatorClassName, Array[Object](this))
    }
  }
  
  override def outputObjectInspector() = soi  
  protected def createOutputOI(): StructObjectInspector
  protected def createCGOperator(): CGOperator
}