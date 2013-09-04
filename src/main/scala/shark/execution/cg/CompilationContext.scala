package shark.execution.cg

import scala.collection.mutable.ArrayBuffer
import shark.execution.cg.operator.CGOperator

class CompilationContext(val useCG: Boolean) {
  var entries = ArrayBuffer[(String, String)]()
  var operators = ArrayBuffer[CGObjectOperator]()
  
  var compilerHelper = new JavaCompilerHelper
  // using the same classloader instance for the "SparkTask", and eventually will gather
  // all of the code gen classes
  var preCompiledClassLoader: OperatorClassLoader = compilerHelper.getOperatorClassLoader
  
  def add(op: CGObjectOperator, opClassName: String, opCode: String) {
    operators +=(op)
    entries += ((opClassName, opCode))
  }
  
  def compile(units: List[(String, String)]) = {
    // TODO the units shouldn't be added into entries(which is a later on compilation list)
    // but the JDK compilation API doens't support the dependencies from ContextClassLoader
    // only thru option -cp in the command line.
    entries ++= units
    
    compilerHelper.compile(units)
    
    preCompiledClassLoader
  }
  
  // to compile all of the source code entries
  def compileAll() = {
    preCompiledClassLoader.clazzCache.clear()
    if(entries.length > 0) compilerHelper.compile(entries.toList)

    preCompiledClassLoader
  }
}