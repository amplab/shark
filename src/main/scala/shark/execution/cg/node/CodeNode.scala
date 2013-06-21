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

package shark.execution.cg.node

import shark.execution.cg.CGContext
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc


/**
 * The generated java source class property definition
 */
class PropertyDefinition(
  var defType: Class[_],
  var defName: String,
  var createInstance: Boolean,
  var isFinal: Boolean = false,
  var isStatic: Boolean = false,
  var initString: String = null) {

  final override def hashCode = 12 * defName.hashCode + 17 * defType.hashCode

  final override def equals(other: Any) = {
    val that = other.asInstanceOf[PropertyDefinition]
    if (that == null) false
    else defName == that.defName && defType == that.defType
  }

  override def toString() = {
    if (createInstance) // if required to create the field instance when declare the field
      "private %s%s %s %s %s;".format(
        if (isFinal) "final " else "",
        if (isStatic) "static " else "",
        defType.getCanonicalName(),
        defName,
        if (initString == null) ("= new " + defType.getCanonicalName() + "()") else initString
      )
    else // if not required to create the field instance when declare the field
      "private %s%s %s = null;".format(
        if (isStatic) "static " else "",
        defType.getCanonicalName(),
        defName
      )
  }
}

/**
 * Delegate Node to generate the complete java class source.
 */
class CodeNode (packageName: String, 
    simpleClassName: String, 
    desc: ExprNodeDesc)
  extends ExprNode[ExprNodeDesc](new CGContext(), desc){
  
  val fullClassName = packageName + "." + simpleClassName
  private var delegate:ExprNode[ExprNodeDesc] = _
  
  protected def getCode() = {
    context.registerImport(classOf[ObjectInspector])
    context.registerImport(classOf[shark.execution.cg.IEvaluate])

    var evaluateCode = delegate.cgEvaluate()
    var value = delegate.valueExpr()
    var validCheck = delegate.cgValidateCheck()

    var code = new StringBuffer
    // 1. define the package name
    code.append("package %s;\n".format(packageName))

    // 2. imports
    context.getImports().foreach(x => code.append("import " + x + "\n"))
    code.append("\n")

    // 3. class definition
    code.append("public class %s extends IEvaluate {\n".format(simpleClassName))

    // 4. fields
    context.getFieldDefinitions().foreach(x => code.append(x + "\n"))

    // 5. init function implementation
    code.append("  @Override\n")
    code.append("  public void init(ObjectInspector %s) {\n".
      format(ColumnNode.INIT_PARAM_NAME))

    delegate.codeInit().foreach(code.append(_).append("\n"))

    code.append("  }\n\n") // end of the init function
    // 6. evaluate function implementation
    code.append("@Override\n")
    code.append("  public Object evaluate(Object %s) {\n".
      format(ColumnNode.EVAL_PARAM_NAME))
    code.append(if(evaluateCode==null) "" else evaluateCode)

    if (delegate.constantNull()) {
      code.append(" return null;\n")
    } else {
      if (null != validCheck) {
        code.append("if(" + validCheck + ") {\n")
        code.append("  return " + value + ";\n")
        code.append("} else {\n")
        code.append("  return null;\n")
        code.append("}\n")
      } else {
        code.append("  return " + value + ";\n")
      }
    }
    code.append("  }\n\n") // end of the evaluate function

    code.append("}\n") // end of the class

    // returns the java source code
    code.toString()
  }

  
  override def prepare(rowInspector: ObjectInspector): Boolean = {
    delegate = context.create(desc).asInstanceOf[ExprNode[ExprNodeDesc]]
    codeEvaluateSnippet = () => getCode()
    
    var result = delegate.fold(rowInspector)
    setOutputInspector(delegate.getOutputInspector())
    
    result
  }

  // Current node is stateful if current UDF or one of the child node is stateful
  override def isStateful() = delegate.isStateful()

  // Current node is deterministic, if current UDF and all children nodes are deterministic 
  override def isDeterministic() = delegate.isDeterministic()
  
  override def notifyEvaluatingCodeGenNeed() { 
    throw new UnsupportedOperationException("notifyEvaluatingCodeGenNeed")
  }
  override def notifycgValidateCheckCodeNeed() {
    throw new UnsupportedOperationException("notifycgValidateCheckCodeNeed")
  }
  override def evaluationType() = delegate.evaluationType()
  override def invalidValueExpr() = throw new UnsupportedOperationException("invalidValueExpr")
  override def initValueExpr() = throw new UnsupportedOperationException("initValueExpr")
  override def resultVariableName() = delegate.resultVariableName()
  override def codeInit() = throw new UnsupportedOperationException("codeInit")
  override def constantNull() = delegate.constantNull()
}