package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.scalatest.FunSuite

import org.apache.hadoop.io.Text

import shark.memstore2.column.Implicits._

class CompressedColumnIteratorSuite extends FunSuite {
      
  test("RLE Decompression") {
    val b = ByteBuffer.allocate(1024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(STRING.typeID)
    val rle = new RLE()
    
    Array(new Text("abc"), new Text("abc"), new Text("efg"), new Text("abc")).foreach { text =>
      STRING.append(text, b)
      rle.gatherStatsForCompressibility(text, STRING)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, STRING)
    val iter = new TestIterator(compressedBuffer, compressedBuffer.getInt())
    iter.next()
    assert(iter.current.toString().equals("abc"))
    iter.next()
    assert(iter.current.toString().equals("abc"))
    assert(iter.current.toString().equals("abc"))
    iter.next()
    assert(iter.current.toString().equals("efg"))
    iter.next()
    assert(iter.current.toString().equals("abc"))
  }
  
  test("Dictionary Decompression") {

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
        u.append(item.asInstanceOf[T], b)
        de.gatherStatsForCompressibility(item, u.asInstanceOf[ColumnType[Any, _]])
      }
      b.limit(b.position())
      b.rewind()
      val compressedBuffer = de.compress(b, u)
      val iter = new TestIterator(compressedBuffer, compressedBuffer.getInt())
      l.foreach { x =>
        iter.next()
        assert(compareFunc(iter.current.asInstanceOf[T], x))
      }
      assert(false === iter.hasNext) // no extras at the end
    }

    val iList = Array[Int](10, 10, 20, 10)
    val lList = iList.map { i => i.toLong }
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
}

 class TestIterator(val buffer: ByteBuffer, val columnType: ColumnType[_,_])
      extends CompressedColumnIterator
