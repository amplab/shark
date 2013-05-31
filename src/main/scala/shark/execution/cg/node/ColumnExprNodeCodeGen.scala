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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils

import shark.execution.cg.CGContext
import shark.execution.cg.ValueType

class ColumnExprNodeCodeGen(context : CGContext, desc : ExprNodeColumnDesc) 
  extends ExprNodeCodeGen[ExprNodeColumnDesc](context, desc) {
  private def init(rowInspector : ObjectInspector) : ObjectInspector = {
    rowInspector match {
      case ooi : StructObjectInspector => {
        // We need to support field names like KEY.0, VALUE.1 between map-reduce boundary.
        var outputOI : ObjectInspector = ooi
        var names : Array[String] = getDesc().getColumn().split("\\.")
        var unionfields : Array[String] = names(0).split("\\:")
        var inspectorNames : Array[String] = new Array[String](names.length + 1)

        var sfFields : Array[StructField] = new Array[StructField](names.length)
        var sfFieldNames : Array[String] = new Array[String](names.length)
        var fieldNames : Array[String] = new Array[String](names.length)

        var unionFields : Array[Int] = new Array[Int](names.length)
        /* 1. extract the field names and their corresponding ObjectInspectors */
        inspectorNames(0) = "soi"

        for (i <- 0 until names.length) {
          unionfields = names(i).split("\\:")
          var fieldName : String = unionfields(0)
          fieldNames(i) = fieldName

          sfFields(i) = outputOI.asInstanceOf[StructObjectInspector].getStructFieldRef(fieldName)
          sfFieldNames(i) = if (i == 0) ("sf_" + fieldName) else (sfFieldNames(i - 1) + "_" + fieldName)

          outputOI = sfFields(i).getFieldObjectInspector()
          inspectorNames(i + 1) = (inspectorNames(i) + "_" + fieldName)
          if (unionfields.length > 1) {
            // contains the union
            unionFields(i) = Integer.parseInt(unionfields(1))
            outputOI = (outputOI.asInstanceOf[UnionObjectInspector]).getObjectInspectors().
                              get(unionFields(i)).asInstanceOf[StructObjectInspector]
          } else {
            unionFields(i) = -1
          }
        }

        /* 2. generate code for the variables definition */
        /* the code may looks like:
        private  org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector soi= null;
        private  org.apache.hadoop.hive.serde2.objectinspector.StructField sf_countrycode= null;
        */
        for (i <- 0 until names.length) {
          getContext().createValueVariableName(ValueType.TYPE_VALUE, 
              classOf[org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector], inspectorNames(i), false)
          getContext().createValueVariableName(ValueType.TYPE_VALUE, 
              classOf[org.apache.hadoop.hive.serde2.objectinspector.StructField], sfFieldNames(i), false)
        }
        
        /* 3. generate code for the field value extraction (inside of the public void init(ObjectInspector oi) */
        /* the code may looks like:
        @Override
        public void init(ObjectInspector oi) {
           this.soi = ((org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector)oi);
           this.sf_countrycode = soi.getStructFieldRef("countrycode");
           this.soi_countrycode = (org.apache.hadoop.hive.serde2.objectinspector.primitive.
                                         WritableStringObjectInspector)sf_countrycode.getFieldObjectInspector();
        }
        */
        codeInit.add("this.soi = ((org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector)oi);\n");
        for (i <- 0 until names.length) {
          codeInit.add("this.%s = %s.getStructFieldRef(\"%s\");\n".
            format(sfFieldNames(i), inspectorNames(i), fieldNames(i)))
          codeInit.add("this.%s = (%s)%s.getFieldObjectInspector();\n".
            format(inspectorNames(i + 1), sfFields(i).getFieldObjectInspector().getClass().getName(), sfFieldNames(i)))

          if (unionFields(i) != -1) {
            codeInit.add("this.%s=(StructObjectInspector)((UnionObjectInspector)%s).getObjectInspectors().get(%s);\n".
              format(inspectorNames(i + 1), inspectorNames(i + 1), unionFields(i)))
          }
        }
        
        /* 4. generate code for the current column variable definition */
        /* the code may looks like:
         * private  org.apache.hadoop.io.Text column_sf_countrycode= null;
         * */
        // TODO currently only support the primitive category
        if (outputOI.isInstanceOf[PrimitiveObjectInspector] == false) {
          logWarning("[" + outputOI.getClass().getCanonicalName() + "] is not PrimitiveObjectInspector. (currently doesn't support Map/Array")
          return null
        }
        variableName = "column_" + sfFieldNames(names.length - 1)
        getContext().createValueVariableName(ValueType.TYPE_VALUE, outputOI.getClass(), 
            inspectorNames(names.length), false)

        // save the current column variable type for its parent node expr evaluation 
        this.variableType = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
          (outputOI.asInstanceOf[PrimitiveObjectInspector]).getPrimitiveCategory()).
              primitiveWritableClass

        /* 5. generate the code for extracting the value of currently column */
        /* the code may looks like:
         * @Override
         * public Object evaluate(Object data) {
         *    column_sf_countrycode=(org.apache.hadoop.io.Text)
         *        soi_countrycode.getPrimitiveWritableObject(soi.getStructFieldData(data, sf_countrycode));
         *    .....
         * */
        var tmpCodeEvaluate : String = "data"
        for (i <- 0 until names.length) {
          tmpCodeEvaluate = inspectorNames(i) + ".getStructFieldData(" + tmpCodeEvaluate + ", " + sfFieldNames(i)

          if (unionFields(i) != -1) {
            tmpCodeEvaluate = "((StandardUnion)" + tmpCodeEvaluate + ").getObject()"
          }
          tmpCodeEvaluate += ")"
        }

        var codeEvaluate : StringBuffer = new StringBuffer()

        if (!getContext().isVariableDefined(resultVariableName)) {
          getContext().createValueVariableName(ValueType.TYPE_VALUE, resultVariableType(), resultVariableName, false)
          codeEvaluate.append("%s=(%s)%s.getPrimitiveWritableObject(%s);\n".
            format(resultVariableName, resultVariableType().getCanonicalName(), inspectorNames(names.length), tmpCodeEvaluate))
        }
        // set the code snippet into "evaluate()"
        this.setCodeEvaluate(codeEvaluate.toString())

        outputOI
      }
      case _ => logWarning("[" + rowInspector.getClass().getCanonicalName() + 
          "] is not PrimitiveObjectInspector. (currently doesn't support Map/Array"); null
    }
  }

  private var variableType : Class[_] = _
  private var variableName : String = _
  private var inits : LinkedHashSet[String] = new LinkedHashSet[String]()

  override def create(rowInspector : ObjectInspector) = {
    init(rowInspector)
  }

  override def resultVariableType() = variableType
  override def resultVariableName() = variableName
  override def codeInit() = inits
}