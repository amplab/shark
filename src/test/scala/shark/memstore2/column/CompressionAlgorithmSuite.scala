package shark.memstore2.column

import org.scalatest.FunSuite
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import shark.memstore2.column._
import shark.memstore2.column.ColumnStats._
import java.nio.ByteBuffer
import java.nio.ByteOrder

class CompressionAlgorithmSuite extends FunSuite {

  test("Compressed Column Builder") {
    class TestColumnBuilder(val stats: ColumnStats[Int], val t: ColumnType[Int])
    extends CompressedColumnBuilder[Int] {
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
    assert(compressedBuffer.getInt() == RLECompressionType.index)
    assert(compressedBuffer.getInt() == INT.index)
    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 2)
    
  }
  test("RLE Strings") {
    val b = ByteBuffer.allocate(1024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(STRING.index)
    STRING.append(new Text("abc"), b)
    STRING.append(new Text("abc"), b)
    STRING.append(new Text("efg"), b)
    STRING.append(new Text("abc"), b)
    b.limit(b.position())
    b.rewind()
    val rle = new RLE() {
      override def sizeOnCompression = STRING.actualSize(new Text("abc"))*3 + 3*4
    }
    val compressedBuffer = rle.compress(b, STRING)
    //first 4 bytes is the compression scheme
    assert(compressedBuffer.getInt() == RLECompressionType.index)
    assert(compressedBuffer.getInt() == STRING.index)
    assert(STRING.extract(compressedBuffer.position(), compressedBuffer).equals (new Text("abc")))
    assert(compressedBuffer.getInt() == 2)
    assert(STRING.extract(compressedBuffer.position(), compressedBuffer).equals (new Text("efg")))
    assert(compressedBuffer.getInt() == 1)
  }
  test("RLE no encoding") {
    val b = ByteBuffer.allocate(16)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.index)
    b.putInt(123)
    b.putInt(56)
    b.limit(b.position())
    b.rewind()
    val rle = new RLE() {
      override def sizeOnCompression = 16
    }
    val compressedBuffer = rle.compress(b, INT)
    //first 4 bytes is the compression scheme
    assert(compressedBuffer.getInt() == RLECompressionType.index)
    assert(compressedBuffer.getInt() == INT.index)
    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 1)
    assert(compressedBuffer.getInt() == 56)
    assert(compressedBuffer.getInt() == 1)
  }
  
  test("RLE perfect encoding") {
    val b = ByteBuffer.allocate(4004)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.index)
    Range(0,1000).foreach { x => b.putInt(6)}
    b.limit(b.position())
    b.rewind()
    val rle = new RLE() {
      override def sizeOnCompression = 8
    }
    val compressedBuffer = rle.compress(b, INT)
    //first 4 bytes is the compression scheme
    assert(compressedBuffer.getInt() == RLECompressionType.index)
    assert(compressedBuffer.getInt() == INT.index)
    assert(compressedBuffer.getInt() == 6)
    assert(compressedBuffer.getInt() == 1000)
    
  }
  
  test("RLE mixture") {
    val b = ByteBuffer.allocate(4004)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.index)
    val items = Array[Int](10, 20, 40)
    Range(0,1000).foreach { x => 
     val v = if (x < 100) items(0) else if (x < 500) items(1) else items(2)  
     b.putInt(v)
    }
    b.limit(b.position())
    b.rewind()
    val rle = new RLE() {
      override def sizeOnCompression = 24
    }
    val compressedBuffer = rle.compress(b, INT)
    //first 4 bytes is the compression scheme
    assert(compressedBuffer.getInt() == RLECompressionType.index)
    assert(compressedBuffer.getInt() == INT.index)
    assert(compressedBuffer.getInt() == 10)
    assert(compressedBuffer.getInt() == 100)
    assert(compressedBuffer.getInt() == 20)
    assert(compressedBuffer.getInt() == 400)
  }
  
  test("RLE perf") {
    val b = ByteBuffer.allocate(4000004)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.index)
    Range(0,1000000).foreach { x => b.putInt(6)}
    b.limit(b.position())
    b.rewind()
    val rle = new RLE() {
      override def sizeOnCompression = 8
    }
    val compressedBuffer = rle.compress(b, INT)
    //first 4 bytes is the compression scheme
    assert(compressedBuffer.getInt() == RLECompressionType.index)
    assert(compressedBuffer.getInt() == INT.index)
    assert(compressedBuffer.getInt() == 6)
    assert(compressedBuffer.getInt() == 1000000)
    
  }
}