package shark.execution.serialization

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.serde2.objectinspector.
{ObjectInspector => OI, PrimitiveObjectInspector => PrimitiveOI}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils => OIUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.{ObjectInspectorCopyOption => CopyOption}
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable

object JoinUtil {

  def computeJoinKey(row: Any,
    keyFields: List[ExprNodeEvaluator], keyFieldsOI: List[OI]): Seq[SerializableWritable[_]] = {
    Range(0, keyFields.size).
      map(i => {
        val c = copy(row, keyFields(i), keyFieldsOI(i), CopyOption.WRITABLE)
        val s = if (c == null) NullWritable.get else c
        new SerializableWritable(s.asInstanceOf[Writable])
      })
  }
  
  def joinKeyHasAnyNulls(joinKey: Seq[AnyRef], nullSafes: Array[Boolean]) : Boolean = {
    joinKey.zipWithIndex.exists(x => {
      (nullSafes == null || nullSafes(x._2).unary_!) && (x._1 == null)
    })
  }

  def computeJoinValues(row: Any,
     valueFields: List[ExprNodeEvaluator], 
     valueFieldsOI: List[OI],
     filters: List[ExprNodeEvaluator],
     filtersOI: List[OI],
     noOuterJoin: Boolean): Array[SerializableWritable[_]] = {
    

    val isFiltered: Boolean = {
      filters.zip(filtersOI).exists(x => {
        val cond = x._1.evaluate(row)
        val result = Option[AnyRef](x._2.asInstanceOf[PrimitiveOI].
        getPrimitiveJavaObject(cond))
        result match {
          case Some(u) => u.asInstanceOf[Boolean].unary_!
          case None => true
        }
      })
    }
    val results = Range(0,valueFields.size).map(i => {
      val c = copy(row, valueFields(i), valueFieldsOI(i), CopyOption.WRITABLE)
      val s = if (c == null) NullWritable.get else c

      new SerializableWritable(s.asInstanceOf[Writable])
    })

    if (noOuterJoin) {
      results.toArray[SerializableWritable[_]]
    } else {
      val s = new SerializableWritable(new BooleanWritable(isFiltered))
      (results ++ List(s)).toArray[SerializableWritable[_]]
    }
  }
  
  private def copy(row: Any, evaluator: ExprNodeEvaluator, oi: OI, copyOption: CopyOption) = {
    OIUtils.copyToStandardObject(evaluator.evaluate(row), oi, copyOption)
  }
}