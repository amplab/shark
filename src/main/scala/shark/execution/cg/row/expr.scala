package shark.execution.cg.row

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }

object TENInstance {
  def create(names: Seq[String], 
      exprs: Seq[ExprNodeDesc], 
      input: CGStruct, 
      output: CGStruct = null): TypedExprNode = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input, null)

    val fields: Seq[TENOutputField] = if (output != null) {
      // sweep the constant expression in the root
      output.fields.zip(exprs).filter((entry) => !(entry._1.constant)).map((entry) => {
        factory.create(entry._1.oiName, entry._2, entry._1, rowInput)
      })
    } else {
      // sweep the constant expression in the root
      import org.apache.hadoop.hive.ql.plan.{ExprNodeConstantDesc, ExprNodeNullDesc}
      names.zip(exprs).filter((entry) => {
        !(entry._2.isInstanceOf[ExprNodeConstantDesc] || entry._2.isInstanceOf[ExprNodeNullDesc])
      }).map((entry) => factory.create(entry._1, entry._2, null, rowInput))
    }

    import scala.collection.JavaConversions._

    val dt = if (output == null) {
      TypeUtil.getDataType(
        OIF.getStandardStructObjectInspector(
          fields.map(_.attr),
          fields.map(_.outputDT.oi.asInstanceOf[OI]))).asInstanceOf[CGStruct]
    } else {
      output
    }

    TENOutputRow(fields, dt)
  }
  
  def create(filter: ExprNodeDesc, expectedDT: DataType, input: CGStruct)
  : TENOutputExpr = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input, null)

    factory.create(filter, expectedDT, rowInput)
  }  
  
  def transform(node: TypedExprNode, ctx: CGExprContext) = {
  	val rule = new RuleValueGuard()
  	val rotated = rule.rotate(node, null, false)
  	val row = rule.cse(rotated, new PathNodeContext(), null)
  	
    row.initialAll(ctx)
    
    row
  }
}

