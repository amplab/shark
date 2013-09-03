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
  self: Operator[_<: HiveDesc] => 
  @transient var soi: StructObjectInspector = _
  @transient var compiler: JavaCompilerHelper = _
  @transient var row: CGStruct = _
  
  // the following field will be ser/de by OperatorSerializationWrapper
  @transient var clcompiler: OperatorClassLoader = _ // classloader for CGed classes
  @transient var cgexec: OperatorExecutor = _
  @BeanProperty var cg: Boolean = _
  @BeanProperty var execClassName: String = _

  private def parentCL(parents: ArrayBuffer[Operator[_ <: HiveDesc]]): ArrayBuffer[OperatorClassLoader] = {
    var compilers = new ArrayBuffer[OperatorClassLoader]()
    if (parents != null) {
      parents.foreach(p => {
        // TODO need to collect all of its parents complier
        compilers ++= parentCL(p.parentOperators)
        if (p.isInstanceOf[CGObjectOperator]) {
          compilers += (p.asInstanceOf[CGObjectOperator].clcompiler)
        }
      }
      )
    }

    compilers
  }
  
  def initCGOnMaster() {
    soi   = createOutputOI()
    cg = useCG
    if (cg) {
      compiler = new JavaCompilerHelper()
      clcompiler = compiler.getOperatorClassLoader()
      
      var oldCL = Thread.currentThread().getContextClassLoader()
      try{
        clcompiler.addClassEntries(parentCL(parentOperators).toArray)
        Thread.currentThread().setContextClassLoader(clcompiler)
        
        var compilationEntries = ArrayBuffer[(String, String)]()
        row = CGField.create(soi)
        var cgoi = CGOIField.create(row).asInstanceOf[CGOIStruct]
        var op = createCGOperator(row, cgoi)
        
        compilationEntries.+=((row.cgClassName, CGRow.generate(row, true) ))
        compilationEntries.+=((cgoi.cgClassName, CGOI.generateOI(cgoi, true)))
        compilationEntries.+=((op.fullClassName, CGOperator.generate(op)))
        
        compiler.compile(compilationEntries)
        execClassName = op.fullClassName
        soi = CGBeanUtils.instance[StructObjectInspector](cgoi.cgClassName)
        
      } finally {
        //Thread.currentThread().setContextClassLoader(oldCL)
      }
    }
  }
  
  def initCGOnSlave() {
    if(cg) {
      cgexec = CGBeanUtils.instance[OperatorExecutor](execClassName, Array[Object](self))
    }
  }
  protected[this] def createOutputOI(): StructObjectInspector
  protected[this] def createCGOperator(cgrow: CGStruct, cgoi: CGOIStruct): CGOperator
}