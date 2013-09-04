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

trait CGObjectOperator {
  // for describing the output data
  @transient var row: CGStruct = _
  @transient var cgexec: OperatorExecutor = _
  
  @BeanProperty var useCG: Boolean = _
  @BeanProperty var operatorClassName: String = _
  // soi will be serialized by Kryo in OperatorSerializationWrapper
  @transient var soi: StructObjectInspector = _
  @transient var operatorCL: OperatorClassLoader = _ // classloader for CGed classes
  
  def initCGOnMaster(cc: CompilationContext) {
    soi   = createOutputOI()
    this.useCG = cc.useCG
    
    if (this.useCG) {
      row = CGField.create(soi)
      var oi = CGOIField.create(row).asInstanceOf[CGOIStruct]
      
      // compile the row and oi object, which will be used while initiliazeOnMaster() 
      operatorCL = cc.compile(List(
          (row.cgClassName, CGRow.generate(row, true)), 
          (oi.cgClassName, CGOI.generateOI(oi, true))))
      
      Thread.currentThread().setContextClassLoader(operatorCL)
      soi = CGBeanUtils.instance[StructObjectInspector](oi.cgClassName)
      
      var operator = createCGOperator()
      operatorClassName = operator.fullClassName
      
      // Put the CG Operator Compilation Unit into context, all of the CG operators 
      // will be compiled together later on  
      cc.add(this, operatorClassName, CGOperator.generate(operator))
    }
  }
  
  def initCGOnSlave() {
    if(this.useCG) {
      cgexec = CGBeanUtils.instance[OperatorExecutor](operatorClassName, Array[Object](this))
    }
  }
  
  protected def createOutputOI(): StructObjectInspector
  protected def createCGOperator(): CGOperator
}