package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Text


trait ColumnStats[T] {

  def init: Unit = ()

  def close: Unit = ()

  def add(v: T)
}


trait RangeStats[T]
  extends ColumnStats[T] {
  var max: T
  var min: T
  def add(v: T)
}


object RangeStats {

  class NumericRangeStats[T <: AnyVal](
      implicit val comparer: SpecializedComparer[T],
      implicit val valueBound: NumericTypeValueBound[T])
    extends RangeStats[T] {
    override var max = valueBound.min
    override var min = valueBound.max
    override def add(v: T) {
      if (comparer.gt(v, max)) max = v
      else if (comparer.lt(v, min)) min = v
    }
  }

  class TextRangeStats extends RangeStats[Text] {
    override var max: Text = null
    override var min: Text = null
    override def add(v: Text) {
      // Need to make a copy of Text since Text is not immutable and we reuse
      // the same Text object in serializer to mitigate frequent GC.
      if (max == null || v.compareTo(max) > 0) max = new Text(v)
      if (min == null || v.compareTo(min) < 0) min = new Text(v)
    }
  }

  class StringRangeStats extends RangeStats[String] {
    override var max: String = null
    override var min: String = null
    override def add(v: String) {
      if (max == null || v > max) max = v
      if (min == null || v < min) min = v
    }
  }
}
