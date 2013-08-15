package shark.memstore2.column

import org.scalatest.FunSuite
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import shark.memstore2.column._
import shark.memstore2.column.ColumnStats._
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class CompressionAlgorithmSuite extends FunSuite {

  test("Compressed Column Builder") {
    class TestColumnBuilder(val stats: ColumnStats[Int], val t: ColumnType[Int,_])
    extends CompressedColumnBuilder[Int] {
      compressionSchemes = Seq(new RLE())
      override def shouldApply(scheme: CompressionAlgorithm) = true
    }
    val b = new TestColumnBuilder(new NoOp, INT)
    b.initialize(100)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    b.append(123.asInstanceOf[Object], oi)
    b.append(123.asInstanceOf[Object], oi)
    b.append(56.asInstanceOf[Object], oi)
    b.append(56.asInstanceOf[Object], oi)
    val compressedBuffer = b.build
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
    Array[Text](new Text("abc"),
        new Text("abc"),
        new Text("efg"),
        new Text("abc")).foreach { text =>
      STRING.append(text, b)
      rle.gatherStatsForCompressability(text, STRING)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, STRING)
    assert(compressedBuffer.getInt() == STRING.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(STRING.extract(compressedBuffer.position(), compressedBuffer).equals (new Text("abc")))
    assert(compressedBuffer.getInt() == 2)
    assert(STRING.extract(compressedBuffer.position(), compressedBuffer).equals (new Text("efg")))
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
    rle.gatherStatsForCompressability(123, INT)
    rle.gatherStatsForCompressability(56, INT)
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 1)
    assert(compressedBuffer.getInt() == 56)
    assert(compressedBuffer.getInt() == 1)
  }
  
  test("RLE perfect encoding") {
    val b = ByteBuffer.allocate(4008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val rle = new RLE()
    Range(0,1000).foreach { x => 
      b.putInt(6)
      rle.gatherStatsForCompressability(6, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 6)
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
     rle.gatherStatsForCompressability(v, INT)
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
      rle.gatherStatsForCompressability(6, INT)
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
    val b = ByteBuffer.allocate(1024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(STRING.typeID)
    val de = new DictionaryEncoding()
    Array[Text](new Text("abc"),
      new Text("abc"),
      new Text("efg"),
      new Text("abc")).foreach { text =>
        STRING.append(text, b)
        de.gatherStatsForCompressability(text, STRING)
      }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = de.compress(b, STRING)
    assert(compressedBuffer.getInt() == STRING.typeID)
    assert(compressedBuffer.getInt() == DictionaryCompressionType.typeID)
    assert(compressedBuffer.getInt() == 2) //dictionary size
    val dictionary = new HashMap[Int, Text]()
    var count = 0
    while (count < 2) {
      val v = STRING.extract(compressedBuffer.position(), compressedBuffer)
      val index = compressedBuffer.getInt()
      dictionary.put(index, v)
      count += 1
    }
    assert(dictionary.get(0).get.equals(new Text("abc")))
    assert(dictionary.get(1).get.equals(new Text("efg")))
    //read the next 4 items
    assert(compressedBuffer.getInt() == 0)
    assert(compressedBuffer.getInt() == 0)
    assert(compressedBuffer.getInt() == 1)
    assert(compressedBuffer.getInt() == 0)
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
    val newBuffer = b.build
    assert(newBuffer.getInt() == STRING.typeID)
    assert(newBuffer.getInt() == RLECompressionType.typeID)
    
  }
}