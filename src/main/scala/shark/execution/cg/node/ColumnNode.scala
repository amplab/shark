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

import scala.collection.JavaConversions.asList
import scala.collection.mutable.LinkedHashSet
import scala.reflect.BeanProperty

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion

import shark.execution.cg.CGContext
import shark.execution.cg.ValueType
import shark.execution.cg.CGAssertRuntimeException
import shark.LogHelper


/**
 * ColumnNode generates the code snippet for retrieving the field value
 */
class ColumnNode(context: CGContext, desc: ExprNodeColumnDesc)
  extends ExprNode[ExprNodeColumnDesc](context, desc) {

  /**
   * As the column could like "a:1.b:1.c:2", we need to extract the ObjectInspector,
   * StructField or UnionObjectInspector by go thru the expr in sequence.
   * The tail "Column" represents the whole expression value.
   *
   * Please also refs the class org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator
   */
  sealed abstract class Column(protected val pre: Option[Column], protected val name: String) {
    @BeanProperty var suffix: String = _ // the suffix of inspector/StructField etc. var names 
    @BeanProperty var inspectorName: String = _ // the inspector variable name
    @BeanProperty var columnName: String = _ // the column variable name

    @BeanProperty var output: ObjectInspector = null // the output ObjectInspector
    protected def init(input: ObjectInspector, context: CGContext): LinkedHashSet[String]

    // get the evaluating code for the current "column"
    def evalCode(): String

    final def initialChain(input: ObjectInspector, context: CGContext): LinkedHashSet[String] = {
      var list = new LinkedHashSet[String]()

      pre match {
        case Some(x) => {
          list ++= x.initialChain(input, context)
          list ++= init(x.getOutput(), context)
        }
        case None => list ++= init(input, context)
      }

      list
    }
  }

  /**
   * Companion object of the Column
   */
  object Column extends LogHelper {
    private val r = """([^.:]*)[:]?(\d*)\.?(.*)""".r
    private def extract(pre: Option[Column], str: String): Option[Column] = {
      // "a:1.b:1.c:2" ==> (a):(1).(b:1.c:2) ==> field:(a), union:(1) rest:(b:1.c:2)
      val r(field, union, rest) = str

      if (!StringUtils.isEmpty(field)) {
        var tail: Column = Field(pre, field)
        tail = if (!StringUtils.isEmpty(union)) {
          Union(Some(tail), union)
        } else {
          tail
        }
        //recursively proceeding the "rest"
        extract(Some(tail), rest)
      } else {
        pre
      }
    }

    def unapply(str: String): Option[Column] = extract(None, str)

    def getRootObjectInspectorClass(clazz: Class[_]) =
      if (classOf[StructObjectInspector].isAssignableFrom(clazz))
        classOf[StructObjectInspector]
      else if (classOf[PrimitiveObjectInspector].isAssignableFrom(clazz))
        classOf[PrimitiveObjectInspector]
      else if (classOf[UnionObjectInspector].isAssignableFrom(clazz))
        classOf[UnionObjectInspector]
      else
        clazz
  }

  case class Field(override val pre: Option[Column], override val name: String)
    extends Column(pre, name) {
    private val SOI = "soi"
    @BeanProperty var sfName: String = _

    private def initVariableNames(uniqueName: String, context: CGContext, oiClass: Class[_]) {
      suffix = uniqueName
      inspectorName = "oi_" + suffix
      sfName = "sf_" + suffix
      columnName = "column_" + suffix

      inspectorName = context.createValueVariableName(ValueType.TYPE_VALUE,
        oiClass,
        inspectorName,
        false
      )

      sfName = context.createValueVariableName(ValueType.TYPE_VALUE,
        classOf[StructField],
        sfName,
        false
      )
    }

    override protected def init(input: ObjectInspector, context: CGContext): 
      LinkedHashSet[String] = {
      
      var list = new LinkedHashSet[String]()
      this.output = input

      if (!input.isInstanceOf[StructObjectInspector]) {
        logWarning("[%s] is not StructObjectInspector.".format(input.getClass().getCanonicalName()))
        return list
      }

      context.registerImport(classOf[StructObjectInspector])

      var sf = input.asInstanceOf[StructObjectInspector].getStructFieldRef(name)
      var oiClass = Column.getRootObjectInspectorClass(sf.getFieldObjectInspector().getClass())

      pre match {
        case Some(x) => {
          // field name is used to guarantee the uniqueness of the variable suffix
          initVariableNames(x.suffix + "_" + name, context, oiClass)

          list.add("this.%s = %s.getStructFieldRef(\"%s\");\n".
            format(sfName, x.inspectorName, name))
          list.add("this.%s = (%s)%s.getFieldObjectInspector();\n".
            format(inspectorName, oiClass.getName(), sfName))
        }
        case None => {
          var soi = context.createValueVariableName(ValueType.TYPE_VALUE,
            classOf[StructObjectInspector],
            SOI,
            false
          )
          // field name is used to guarantee the uniqueness of the variable suffix
          initVariableNames(name, context, oiClass)
          list.add("this.%s = (StructObjectInspector)%s;\n".
            format(soi, ColumnNode.INIT_PARAM_NAME))
          list.add("this.%s = %s.getStructFieldRef(\"%s\");\n".format(sfName, soi, name))
          list.add("this.%s = (%s)%s.getFieldObjectInspector();\n".
            format(inspectorName, oiClass.getName(), sfName))
        }
      }
      output = sf.getFieldObjectInspector()

      list
    }

    override def evalCode(): String = pre match {
      case Some(x) => {
        "%s.getStructFieldData(%s,%s)".format(x.inspectorName, x.evalCode(), sfName)
      }
      case None => {
        "%s.getStructFieldData(%s,%s)".format(SOI, ColumnNode.EVAL_PARAM_NAME, sfName)
      }
    }
  }

  case class Union(override val pre: Option[Column], override val name: String)
    extends Column(pre, name) {
    override protected def init(input: ObjectInspector, 
                                context: CGContext): LinkedHashSet[String] = {
      var list = new LinkedHashSet[String]()
      this.output = input

      if (!input.isInstanceOf[UnionObjectInspector]) {
        logWarning("[%s] is not UnionObjectInspector. (currently doesn't support Map/Array".
           format(input.getClass().getCanonicalName()))
        return list
      }

      var idx = Integer.parseInt(name)
      output = (input.asInstanceOf[UnionObjectInspector]).getObjectInspectors().get(idx)

      context.registerImport(classOf[StandardUnion])

      var oiClass = Column.getRootObjectInspectorClass(output.getClass())
      pre match {
        case Some(x) => {
          suffix = x.suffix + "_" + idx
          inspectorName = x.inspectorName + "_" + idx
          columnName = x.columnName + "_" + idx

          inspectorName = context.createValueVariableName(ValueType.TYPE_VALUE,
            oiClass,
            inspectorName,
            false
          )

          list.add(
            "this.%s=(%s)(%s.getObjectInspectors().get(%s));\n".
              format(inspectorName, oiClass.getName(), x.inspectorName, idx))
        }
        case None => {
          throw new CGAssertRuntimeException("Shouldn't run into here.")
        }
      }

      list
    }

    override def evalCode(): String = pre match {
      case Some(x) => {
        "%s.getField(%s)".format(x.inspectorName, x.evalCode())
      }
      case None => throw new CGAssertRuntimeException("Union cannot be the first ObjectInspector")
    }
  }

  private def init(rowInspector: ObjectInspector, col: Column) = {
    inits ++= col.initialChain(rowInspector, getContext())
    // set the variable name
    codeWritableNameSnippet = () => getContext().createValueVariableName(
      ValueType.TYPE_VALUE,
      resultVariableType(),
      col.columnName,
      false)

    // set the "evaluate()" code snippet
    codeEvaluateSnippet = () => "%s=(%s)%s.getPrimitiveWritableObject(%s);\n".format(
          resultVariableName(),
          resultVariableType().getCanonicalName(),
          col.inspectorName,
          col.evalCode)

    // set the expr value
    ColumnNode.this.codeValueExpr = ()=>resultVariableName()
    col.getOutput() // output object inspector 
  }
  
  override def prepare(rowInspector: ObjectInspector) = {
    // extract the Field / Union recursively
    val Column(col) = desc.getColumn()
    var primitveOutputInspector = castToPrimitiveObjectInspector(init(rowInspector, col))
    if (primitveOutputInspector != null) {
      setOutputInspector(primitveOutputInspector)
      true
    } else {
      false
    }
  }

  private val inits = new LinkedHashSet[String]()

  override def codeInit() = inits
}

object ColumnNode {
  val INIT_PARAM_NAME = "oi" // generated java source parameter name of "init()"
  val EVAL_PARAM_NAME = "data" // generated java source parameter name of "evaluate()"

  def apply(context: CGContext, desc: ExprNodeColumnDesc) =
    new ColumnNode(context, desc)
}