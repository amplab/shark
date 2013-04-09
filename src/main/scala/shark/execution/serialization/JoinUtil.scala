package shark.execution.serialization

import java.util.{List => JavaList}
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils => OIUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.{ObjectInspectorCopyOption => CopyOption}
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector => PrimitiveOI}
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable

object JoinUtil {

  def computeJoinKey(row: Any,
    keyFields: JavaList[ExprNodeEvaluator], keyFieldsOI: JavaList[OI]): Seq[SerializableWritable[_]] = {
    Range(0, keyFields.size).
      map(i => {
        val c = copy(row, keyFields.get(i), keyFieldsOI.get(i), CopyOption.WRITABLE)
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
     valueFields: JavaList[ExprNodeEvaluator], 
     valueFieldsOI: JavaList[OI],
     filters: JavaList[ExprNodeEvaluator],
     filtersOI: JavaList[OI],
     noOuterJoin: Boolean): Array[AnyRef] = {
    

    val isFiltered: Boolean = {
      Range(0,filters.size()).exists(x => {
        val cond = filters.get(x).evaluate(row)
        val result = Option[AnyRef](filtersOI.get(x).asInstanceOf[PrimitiveOI].
        getPrimitiveJavaObject(cond))
        result match {
          case Some(u) => u.asInstanceOf[Boolean].unary_!
          case None => true
        }
      })
    }
    val size = valueFields.size
    val a = new Array[AnyRef](size)
    Range(0, size).foreach(i => {
      val c = copy(row, valueFields.get(i), valueFieldsOI.get(i), CopyOption.WRITABLE)
      val s = if (c == null) NullWritable.get else c

      a(i) = new SerializableWritable(s.asInstanceOf[Writable])
    })

    if (noOuterJoin) {
      a
    } else {
      val n = new Array[AnyRef](size+1)
      Array.copy(a, 0, n, 0, size)
      n(size) = new SerializableWritable(new BooleanWritable(isFiltered))
      n
    }
  }
  
  private def copy(row: Any, evaluator: ExprNodeEvaluator, oi: OI, copyOption: CopyOption) = {
    OIUtils.copyToStandardObject(evaluator.evaluate(row), oi, copyOption)
  }
}