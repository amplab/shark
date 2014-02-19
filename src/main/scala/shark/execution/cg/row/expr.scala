package shark.execution.cg.row

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }

object TENInstance {
  def create(names: Seq[String], exprs: Seq[ExprNodeDesc], input: CGStruct, output: CGStruct = null): TypedExprNode = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input)

    val fields = if (output != null) {
      Seq.tabulate(exprs.length)(i => {
        factory.create(output.fields(i).oiName, exprs(i), output.fields(i), rowInput)
      })
    } else {
      Seq.tabulate(exprs.length)(i => {
        factory.create(names(i), exprs(i), null, rowInput)
      })
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
  
  def create(filter: ExprNodeDesc, expectedDT: DataType, input: CGStruct): TENOutputExpr = {
    val factory = new TENFactory
    val rowInput = TENInputRow(input)

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

