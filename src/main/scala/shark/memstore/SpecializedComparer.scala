package shark.memstore

trait SpecializedComparer[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T] {
  def gt(x: T, y: T): Boolean
  def lt(x: T, y: T): Boolean
  def eq(x: T, y: T): Boolean
}

object SpecializedComparer {

  implicit object BooleanComparer extends SpecializedComparer[Boolean] {
    override def gt(x: Boolean, y: Boolean) = x & !y
    override def lt(x: Boolean, y: Boolean) = !x & y
    override def eq(x: Boolean, y: Boolean) = !(x ^ y)
  }

  implicit object ByteComparer extends SpecializedComparer[Byte] {
    override def gt(x: Byte, y: Byte) = x > y
    override def lt(x: Byte, y: Byte) = x < y
    override def eq(x: Byte, y: Byte) = x == y
  }

  implicit object ShortComparer extends SpecializedComparer[Short] {
    override def gt(x: Short, y: Short) = x > y
    override def lt(x: Short, y: Short) = x < y
    override def eq(x: Short, y: Short) = x == y
  }

  implicit object IntComparer extends SpecializedComparer[Int] {
    override def gt(x: Int, y: Int) = x > y
    override def lt(x: Int, y: Int) = x < y
    override def eq(x: Int, y: Int) = x == y
  }

  implicit object LongComparer extends SpecializedComparer[Long] {
    override def gt(x: Long, y: Long) = x > y
    override def lt(x: Long, y: Long) = x < y
    override def eq(x: Long, y: Long) = x == y
  }

  implicit object FloatComparer extends SpecializedComparer[Float] {
    override def gt(x: Float, y: Float) = x > y
    override def lt(x: Float, y: Float) = x < y
    override def eq(x: Float, y: Float) = x == y
  }

  implicit object DoubleComparer extends SpecializedComparer[Double] {
    override def gt(x: Double, y: Double) = x > y
    override def lt(x: Double, y: Double) = x < y
    override def eq(x: Double, y: Double) = x == y
  }
}
