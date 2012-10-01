package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Text


/**
 * Column level statistics, including range (min, max).
 */
sealed trait ColumnStats[T] {
  def append(v: T)
  def appendNull() {}
  def min: T
  def max: T
  def build = this
  override def toString = "[" + min + ", " + max + "]"
}


sealed trait ColumnNoStats[T] extends ColumnStats[T]


// For all columns, we track the range (i.e. min and max).
// For int columns, we try to do more since int is more common. Examples include
// ordering of the column and max deltas (max difference between two cells).
object ColumnStats {

  class BooleanColumnStats extends ColumnStats[Boolean] {
    private var _max = false
    private var _min = true
    override def append(v: Boolean) {
      if (v) _max = v
      else _min = v
    }
    override def min = _min
    override def max = _max
  }

  class ByteColumnStats extends ColumnStats[Byte] {
    private var _max = Byte.MinValue
    private var _min = Byte.MaxValue
    override def append(v: Byte) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
    override def min = _min
    override def max = _max
  }

  class ShortColumnStats extends ColumnStats[Short] {
    private var _max = Short.MinValue
    private var _min = Short.MaxValue
    override def append(v: Short) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
    override def min = _min
    override def max = _max
  }

  class IntColumnStats extends ColumnStats[Int] {

    private val UNINITIALIZED = 0  // Haven't seen the first value yet.
    private val INITIALIAZED = 1   // Seen first, and processing second value.
    private val ASCENDING = 2
    private val DESCENDING = 3
    private val UNORDERED = 4
    private var _orderedState = UNINITIALIZED

    private var _max = Int.MinValue
    private var _min = Int.MaxValue
    private var _lastValue = 0
    private var _maxDelta = 0

    override def min = _min
    override def max = _max
    def isAscending = _orderedState != DESCENDING && _orderedState != UNORDERED
    def isDescending = _orderedState != ASCENDING && _orderedState != UNORDERED
    def isOrdered = isAscending || isDescending
    def maxDelta = _maxDelta

    override def append(v: Int) {
      if (v > _max) _max = v
      if (v < _min) _min = v

      if (_orderedState == UNINITIALIZED) {
        // First value.
        _orderedState = INITIALIAZED
        _lastValue = v
      } else if (_orderedState == INITIALIAZED) {
        // Second value.
        _orderedState = if (v >= _lastValue) ASCENDING else DESCENDING
        _maxDelta = math.abs(v - _lastValue)
        _lastValue = v
      } else if (_orderedState == ASCENDING) {
        if (v < _lastValue) _orderedState = UNORDERED
        else {
          if (v - _lastValue > _maxDelta) _maxDelta = v - _lastValue
          _lastValue = v
        }
      } else if (_orderedState == DESCENDING) {
        if (v > _lastValue) _orderedState = UNORDERED
        else {
          if (_lastValue - v > _maxDelta) _maxDelta = _lastValue - v
          _lastValue = v
        }
      }
    }
  }

  class LongColumnStats extends ColumnStats[Long] {
    private var _max = Long.MinValue
    private var _min = Long.MaxValue
    override def append(v: Long) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
    override def min = _min
    override def max = _max
  }

  class FloatColumnStats extends ColumnStats[Float] {
    private var _max = Float.MinValue
    private var _min = Float.MaxValue
    override def append(v: Float) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
    override def min = _min
    override def max = _max
  }

  class DoubleColumnStats extends ColumnStats[Double] {
    private var _max = Double.MinValue
    private var _min = Double.MaxValue
    override def append(v: Double) {
      if (v > _max) _max = v
      if (v < _min) _min = v
    }
    override def min = _min
    override def max = _max
  }

  class TextColumnStats extends ColumnStats[Text] {
    private var _max: Text = null
    private var _min: Text = null
    override def max = _max
    override def min = _min
    override def append(v: Text) {
      // Need to make a copy of Text since Text is not immutable and we reuse
      // the same Text object in serializer to mitigate frequent GC.
      if (max == null || v.compareTo(_max) > 0) _max = new Text(v)
      if (min == null || v.compareTo(_min) < 0) _min = new Text(v)
    }
  }

  implicit object BooleanColumnNoStats extends ColumnStats[Boolean] {
    override def append(v: Boolean) {}
    override def min = false
    override def max = false 
  }

  implicit object ByteColumnNoStats extends ColumnStats[Byte] {
    override def append(v: Byte) {}
    override def min = 0
    override def max = 0 
  }

  implicit object ShortColumnNoStats extends ColumnStats[Short] {
    override def append(v: Short) {}
    override def min = 0
    override def max = 0 
  }

  implicit object IntColumnNoStats extends ColumnStats[Int] {
    override def append(v: Int) {}
    override def min = 0
    override def max = 0 
  }

  implicit object LongColumnNoStats extends ColumnStats[Long] {
    override def append(v: Long) {}
    override def min = 0
    override def max = 0 
  }

  implicit object FloatColumnNoStats extends ColumnStats[Float] {
    override def append(v: Float) {}
    override def min = 0
    override def max = 0 
  }

  implicit object DoubleColumnNoStats extends ColumnStats[Double] {
    override def append(v: Double) {}
    override def min = 0
    override def max = 0 
  }

  implicit object TextColumnNoStats extends ColumnStats[Text] {
    override def append(v: Text) {}
    override def min = null
    override def max = null
  }

  implicit object GenericColumnNoStats extends ColumnStats[Object] {
    override def append(v: Object) {}
    override def min = null
    override def max = null
  }
}
