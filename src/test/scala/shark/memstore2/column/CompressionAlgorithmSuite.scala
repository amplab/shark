package shark.memstore2.column

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.HashMap

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text

import org.scalatest.FunSuite

import shark.memstore2.column.ColumnStats._

class CompressionAlgorithmSuite extends FunSuite {

  // TODO: clean these tests.

  test("Compressed Column Builder") {

    class TestColumnBuilder(val stats: ColumnStats[Int], val t: ColumnType[Int,_])
      extends CompressedColumnBuilder[Int] {
      compressionSchemes = Seq(new RLE())
      override def shouldApply(scheme: CompressionAlgorithm) = true
    }

    val b = new TestColumnBuilder(new NoOpStats, INT)
    b.initialize(100)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    b.append(123.asInstanceOf[Object], oi)
    b.append(123.asInstanceOf[Object], oi)
    b.append(56.asInstanceOf[Object], oi)
    b.append(56.asInstanceOf[Object], oi)
    val compressedBuffer = b.build()
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)

    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 2)
  }

  test("RLE Strings") {
    val b = ByteBuffer.allocate(1024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(STRING.typeID)
    val rle = new RLE()
    Seq[Text](new Text("abc"), new Text("abc"), new Text("efg"), new Text("abc")).foreach { text =>
      STRING.append(text, b)
      rle.gatherStatsForCompressibility(text, STRING)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, STRING)
    assert(compressedBuffer.getInt() == STRING.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(STRING.extract(compressedBuffer.position(), compressedBuffer).equals(new Text("abc")))
    assert(compressedBuffer.getInt() == 2)
    assert(STRING.extract(compressedBuffer.position(), compressedBuffer).equals(new Text("efg")))
    assert(compressedBuffer.getInt() == 1)
  }

  test("RLE no encoding") {
    val b = ByteBuffer.allocate(16)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    b.putInt(123)
    b.putInt(56)
    b.limit(b.position())
    b.rewind()
    val rle = new RLE()
    rle.gatherStatsForCompressibility(123, INT)
    rle.gatherStatsForCompressibility(56, INT)
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 1)
    assert(compressedBuffer.getInt() == 56)
    assert(compressedBuffer.getInt() == 1)
  }
  
  test("RLE perfect encoding Int") {
    val b = ByteBuffer.allocate(4008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val rle = new RLE()
    Range(0,1000).foreach { x => 
      b.putInt(6)
      rle.gatherStatsForCompressibility(6, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 6)
    assert(compressedBuffer.getInt() == 1000)
  }
  
  test("RLE perfect encoding Long") {
    val b = ByteBuffer.allocate(8008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(LONG.typeID)
    val rle = new RLE()
    Range(0,1000).foreach { x => 
      b.putLong(Long.MaxValue - 6)
      rle.gatherStatsForCompressibility(Long.MaxValue - 6, LONG)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, LONG)
    assert(compressedBuffer.getInt() == LONG.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getLong() == Long.MaxValue - 6)
    assert(compressedBuffer.getInt() == 1000)
  }
  
  test("RLE mixture") {
    val b = ByteBuffer.allocate(4008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val items = Array[Int](10, 20, 40)
    val rle = new RLE()

    Range(0,1000).foreach { x => 
      val v = if (x < 100) items(0) else if (x < 500) items(1) else items(2)
      b.putInt(v)
      rle.gatherStatsForCompressibility(v, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 10)
    assert(compressedBuffer.getInt() == 100)
    assert(compressedBuffer.getInt() == 20)
    assert(compressedBuffer.getInt() == 400)
  }

  test("RLE perf") {
    val b = ByteBuffer.allocate(4000008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val rle = new RLE()

    Range(0,1000000).foreach { x => 
      b.putInt(6)
      rle.gatherStatsForCompressibility(6, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    //first 4 bytes is the compression scheme
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == 6)
    assert(compressedBuffer.getInt() == 1000000)
  }
  
  test("Dictionary Encoding") {

    def testList[T](
      l: Seq[T],
      u: ColumnType[T, _],
      expectedDictSize: Int,
      compareFunc: (T, T) => Boolean = (a: T, b: T) => a == b) {

      val b = ByteBuffer.allocate(1024 + (3*40*l.size))
      b.order(ByteOrder.nativeOrder())
      b.putInt(u.typeID)
      val de = new DictionaryEncoding()
      l.foreach { item =>
        u.append(item, b)
        de.gatherStatsForCompressibility(item, u.asInstanceOf[ColumnType[Any, _]])
      }
      b.limit(b.position())
      b.rewind()
      val compressedBuffer = de.compress(b, u)
      assert(compressedBuffer.getInt() === u.typeID)
      assert(compressedBuffer.getInt() === DictionaryCompressionType.typeID)
      assert(compressedBuffer.getInt() === expectedDictSize) //dictionary size
      val dictionary = new HashMap[Short, T]()
      var count = 0
      while (count < expectedDictSize) {
        val v = u.extract(compressedBuffer.position(), compressedBuffer)
        val index = compressedBuffer.getShort()
        dictionary.put(index, u.clone(v))
        count += 1
      }
      assert(dictionary.get(0).get.equals(l(0)))
      assert(dictionary.get(1).get.equals(l(2)))
      l.foreach { x => 
        val y = dictionary.get(compressedBuffer.getShort()).get
        assert(compareFunc(y, x))
      }
    }

    val iList = Array[Int](10, 10, 20, 10)
    val lList = iList.map { i => Long.MaxValue - i.toLong }
    val sList = iList.map { i => new Text(i.toString) }

    testList(iList, INT, 2)
    testList(lList, LONG, 2)
    testList(sList, STRING, 2, (a: Text, b: Text) => a.hashCode == b.hashCode)

    // test at limit of unique values
    val alternating = Range(0, Short.MaxValue-1, 1).flatMap { s => List(1, s) }
    val longList = List.concat(iList, alternating, iList)
    assert(longList.size === (8 + 2*(Short.MaxValue-1)))
    testList(longList, INT, Short.MaxValue - 1)
  }
  
  test("RLE region") {
    val b = new StringColumnBuilder
    b.initialize(0)
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector

    val lines = Array[String]("lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to",
      "hs use ironic, even requests. s",
      "ges. thinly even pinto beans ca",
      "ly final courts cajole furiously final excuse",
      "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl"
    )
    lines.foreach { line =>
      b.append(line, oi)
    }
    val newBuffer = b.build()
    assert(newBuffer.getInt() == STRING.typeID)
    assert(newBuffer.getInt() == RLECompressionType.typeID)
    
  }
}
