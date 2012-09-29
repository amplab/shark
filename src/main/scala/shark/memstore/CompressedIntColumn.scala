package shark.memstore


object CompressedIntColumn {

  val BYTE_RANGE = Byte.MaxValue - Byte.MinValue
  val SHORT_RANGE = Short.MaxValue - Short.MinValue

  def compressIntArray(intArray: Array[Int], min: Int, max: Int): BackingIntArray = {
    val range = max - min
    if (range <= BYTE_RANGE)       new ByteArrayAsIntArray(intArray, min, max)
    else if (range <= SHORT_RANGE) new ShortArrayAsIntArray(intArray, min, max)
    else                           new IntArray(intArray)
  }

  sealed trait BackingIntArray {
    def get(i: Int): Int
  }

  class ByteArrayAsIntArray(intArray: Array[Int], min: Int, max: Int) extends BackingIntArray {
    def this(intArray: Array[Int]) = this(intArray, intArray.min, intArray.max)

    override def get(i: Int): Int = _data(i) + base
    val base = (min + max) / 2

    private val _data = new Array[Byte](intArray.size)
    private def init(intArray: Array[Int], min: Int, max: Int) {
      var i = 0
      while (i < intArray.size) {
        _data(i) = (intArray(i) - base).toByte
        i+= 1
      }
    }
    init(intArray, min, max)
  }

  class ShortArrayAsIntArray(intArray: Array[Int], min: Int, max: Int) extends BackingIntArray {
    def this(intArray: Array[Int]) = this(intArray, intArray.min, intArray.max)

    override def get(i: Int): Int = _data(i) + base
    val base = (min + max) / 2

    private val _data = new Array[Short](intArray.size)
    private def init(intArray: Array[Int], min: Int, max: Int) {
      var i = 0
      while (i < intArray.size) {
        _data(i) = (intArray(i) - base).toShort
        i+= 1
      }
    }
    init(intArray, min, max)
  }

  class IntArray(intArray: Array[Int]) extends BackingIntArray {
    override def get(i: Int): Int = intArray(i)
  }
}
