package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Text


trait ColumnStats[T] {
  def init: Unit = ()
  def close: Unit = ()
  def add(v: T)
  def min: T
  def max: T
}


object ColumnStats {

  class Numeric[T <: AnyVal](
      implicit val comparer: SpecializedComparer[T],
      implicit val valueBound: NumericTypeValueBound[T])
    extends ColumnStats[T] {
  
    private var _max = valueBound.min
    private var _min = valueBound.max
  
    override def max = _max
    override def min = _min
    
    override def add(v: T) {
      if (comparer.gt(v, _max)) _max = v
      else if (comparer.lt(v, _min)) _min = v
    }
  }
  
  class HadoopText extends ColumnStats[Text] {
    
    private var _max: Text = null
    private var _min: Text = null
  
    override def max = _max
    override def min = _min
  
    override def add(v: Text) {
      // Need to make a copy of Text since Text is not immutable and we reuse
      // the same Text object in serializer to mitigate frequent GC.
      if (max == null || v.compareTo(_max) > 0) _max = new Text(v)
      if (min == null || v.compareTo(_min) < 0) _min = new Text(v)
    }
  }

}