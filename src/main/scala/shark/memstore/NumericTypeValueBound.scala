package shark.memstore

/**
 * Trait used to obtain the max and min value for a type. We don't write
 * specialized code for this one since it is used very infrequently.
 */
trait NumericTypeValueBound[T] {
  def max: T
  def min: T
}

object NumericTypeValueBound {

  implicit object BooleanValueBound extends NumericTypeValueBound[Boolean] {
    override def max = true
    override def min = false
  }

  implicit object ByteValueBound extends NumericTypeValueBound[Byte] {
    override def max = Byte.MaxValue
    override def min = Byte.MinValue
  }

  implicit object ShortValueBound extends NumericTypeValueBound[Short] {
    override def max = Short.MaxValue
    override def min = Short.MinValue
  }

  implicit object IntValueBound extends NumericTypeValueBound[Int] {
    override def max = Int.MaxValue
    override def min = Int.MinValue
  }

  implicit object LongValueBound extends NumericTypeValueBound[Long] {
    override def max = Long.MaxValue
    override def min = Long.MinValue
  }

  implicit object FloatValueBound extends NumericTypeValueBound[Float] {
    override def max = Float.MaxValue
    override def min = Float.MinValue
  }

  implicit object DoubleValueBound extends NumericTypeValueBound[Double] {
    override def max = Double.MaxValue
    override def min = Double.MinValue
  }
}
