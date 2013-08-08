package shark.memstore2.column

import org.scalatest.FunSuite
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.hadoop.io.Text

class CompressedColumnIteratorSuite extends FunSuite {
  
  test("RLE Decompression") {
    class TestIterator(val buffer: ByteBuffer) extends CompressedColumnIterator
    
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
    
    val iter = new TestIterator(compressedBuffer)
    iter.next()
    println(iter.current)
  }

}