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

package shark.memstore

import java.io.{ObjectInput, ObjectOutput, Externalizable}

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Text


/**
 * Column level statistics, including range (min, max).
 */
sealed trait ColumnStats[T] extends Serializable {
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

  object IntColumnStats {
    private val UNINITIALIZED = 0  // Haven't seen the first value yet.
    private val INITIALIAZED = 1   // Seen first, and processing second value.
    private val ASCENDING = 2
    private val DESCENDING = 3
    private val UNORDERED = 4
  }

  class IntColumnStats extends ColumnStats[Int] {
    import IntColumnStats._
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

  class TextColumnStats extends ColumnStats[Text] with Externalizable {
    // Note: this is not Java serializable because Text is not Java serializable.
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

    override def readExternal(in: ObjectInput) {
      if (in.readBoolean()) {
        _max = new Text
        _max.readFields(in)
      }
      if (in.readBoolean()) {
        _min = new Text
        _min.readFields(in)
      }
    }

    override def writeExternal(out: ObjectOutput) {
      if (_max == null) {
        out.write(0)
      } else {
        out.write(1)
        _max.write(out)
      }
      if (_min == null) {
        out.write(0)
      } else {
        out.write(1)
        _min.write(out)
      }
    }
  }

  implicit object BooleanColumnNoStats extends ColumnNoStats[Boolean] {
    override def append(v: Boolean) {}
    override def min = false
    override def max = false
  }

  implicit object ByteColumnNoStats extends ColumnNoStats[Byte] {
    override def append(v: Byte) {}
    override def min = 0
    override def max = 0
  }

  implicit object ShortColumnNoStats extends ColumnNoStats[Short] {
    override def append(v: Short) {}
    override def min = 0
    override def max = 0
  }

  implicit object IntColumnNoStats extends ColumnNoStats[Int] {
    override def append(v: Int) {}
    override def min = 0
    override def max = 0
  }

  implicit object LongColumnNoStats extends ColumnNoStats[Long] {
    override def append(v: Long) {}
    override def min = 0
    override def max = 0
  }

  implicit object FloatColumnNoStats extends ColumnNoStats[Float] {
    override def append(v: Float) {}
    override def min = 0
    override def max = 0
  }

  implicit object DoubleColumnNoStats extends ColumnNoStats[Double] {
    override def append(v: Double) {}
    override def min = 0
    override def max = 0
  }

  implicit object TextColumnNoStats extends ColumnNoStats[Text] {
    override def append(v: Text) {}
    override def min = null
    override def max = null
  }

  implicit object GenericColumnNoStats extends ColumnNoStats[Object] {
    override def append(v: Object) {}
    override def min = null
    override def max = null
  }
}
