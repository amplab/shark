package shark.memstore2.column

import org.scalatest.FunSuite
import java.nio.ByteBuffer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.hive.serde2.io._

class ColumnTypeSuite extends FunSuite {

  test("Int") {
    assert(INT.defaultSize == 4)
    var buffer = ByteBuffer.allocate(32)
    var a: Seq[Int] = Array[Int](35, 67, 899, 4569001)
    a.foreach {i => buffer.putInt(i)}
    buffer.rewind()
    a.foreach {i => 
      val v = INT.extract(buffer.position(), buffer)
      assert(v == i)
    }
    buffer = ByteBuffer.allocate(32)
    a = Range(0, 4)
    a.foreach { i => 
      INT.append(i, buffer)  
    }
    buffer.rewind()
    a.foreach { i => assert(buffer.getInt() == i)}
    
    buffer = ByteBuffer.allocate(32)
    a =Range(0,4)
    a.foreach { i => buffer.putInt(i)}
    buffer.rewind()
    val writable = new IntWritable()
    a.foreach { i => 
      INT.extractInto(buffer.position(), buffer, writable)
      assert(writable.get == i)
    }
    
  }
  
  test("Short") {
    assert(SHORT.defaultSize == 2)
    assert(SHORT.actualSize(8) == 2)
    var buffer = ByteBuffer.allocate(32)
    var a = Array[Short](35, 67, 87, 45)
    a.foreach {i => buffer.putShort(i)}
    buffer.rewind()
    a.foreach {i => 
      val v = SHORT.extract(buffer.position(), buffer)
      assert(v == i)
    }
    
    buffer = ByteBuffer.allocate(32)
    a = Array[Short](0,1,2,3)
    a.foreach { i => 
      SHORT.append(i, buffer)  
    }
    buffer.rewind()
    a.foreach { i => assert(buffer.getShort() == i)}
    
    buffer = ByteBuffer.allocate(32)
    a =Array[Short](0,1,2,3)
    a.foreach { i => buffer.putShort(i)}
    buffer.rewind()
    val writable = new ShortWritable()
    a.foreach { i => 
      SHORT.extractInto(buffer.position(), buffer, writable)
      assert(writable.get == i)
    }
  }
  
  test("Long") {
    assert(LONG.defaultSize == 8)
    assert(LONG.actualSize(45L) == 8)
    var buffer = ByteBuffer.allocate(64)
    var a = Array[Long](35L, 67L, 8799000880L, 45000999090L)
    a.foreach {i => buffer.putLong(i)}
    buffer.rewind()
    a.foreach {i => 
      val v = LONG.extract(buffer.position(), buffer)
      assert(v == i)
    }
    
    buffer = ByteBuffer.allocate(32)
    a = Array[Long](0,1,2,3)
    a.foreach { i => 
      LONG.append(i, buffer)  
    }
    buffer.rewind()
    a.foreach { i => assert(buffer.getLong() == i)}
    
    buffer = ByteBuffer.allocate(32)
    a =Array[Long](0,1,2,3)
    a.foreach { i => buffer.putLong(i)}
    buffer.rewind()
    val writable = new LongWritable()
    a.foreach { i => 
      LONG.extractInto(buffer.position(), buffer, writable)
      assert(writable.get == i)
    }
  }
}
