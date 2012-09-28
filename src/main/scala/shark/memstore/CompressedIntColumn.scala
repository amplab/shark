package shark.memstore


object CompressedIntColumn {

  sealed trait BackingIntArray {
    def get(i: Int): Int
  }

  class ByteArrayAsIntArray(intArray: Array[Int], min: Int, max: Int) extends BackingIntArray {

    def this(intArray: Array[Int]) = this(intArray, intArray.min, intArray.max)

    val base = (min + max) / 2

    private val _data = new Array[Byte](intArray.size)

    override def get(i: Int): Int = _data(i) + base

    init(intArray, min, max)
    private def init(intArray: Array[Int], min: Int, max: Int) {
      var i = 0
      while (i < intArray.size) {
        _data(i) = (intArray(i) - base).toByte
        i+= 1
      }
    }
  }

  class ShortArrayAsIntArray(intArray: Array[Int], min: Int, max: Int) extends BackingIntArray {

    def this(intArray: Array[Int]) = this(intArray, intArray.min, intArray.max)

    val base = (min + max) / 2

    private val _data = new Array[Short](intArray.size)

    override def get(i: Int): Int = _data(i) + base

    init(intArray, min, max)
    private def init(intArray: Array[Int], min: Int, max: Int) {
      var i = 0
      while (i < intArray.size) {
        _data(i) = (intArray(i) - base).toShort
        i+= 1
      }
    }
  }
}
