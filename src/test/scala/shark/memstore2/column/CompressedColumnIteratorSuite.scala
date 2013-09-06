package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.scalatest.FunSuite

import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.memstore2.column.Implicits._

class CompressedColumnIteratorSuite extends FunSuite {

  val booleanList = Array[Boolean](true, true, false, true)
  val byteList = Array[Byte](10, 10, 20, 10)
  val shortList = byteList.map { i => (Short.MaxValue - i).toShort }
  val iList = byteList.map { i => Int.MaxValue - i.toInt }
  val lList = iList.map { i => Long.MaxValue - i.toLong }
  val sList = iList.map { i => new Text(i.toString) }

  /** Generic tester across types and encodings
    *
    */
  def testList[T, W](
    l: Seq[T],
    u: ColumnType[T, _],
    algo: CompressionAlgorithm,
    compareFunc: (T, T) => Boolean = (a: T, b: T) => a == b,
    shouldNotCompress: Boolean = false) {

    val b = ByteBuffer.allocate(1024 + (3*40*l.size))
    b.order(ByteOrder.nativeOrder())
    b.putInt(u.typeID)
    l.foreach { item =>
      u.append(item.asInstanceOf[T], b)
      algo.gatherStatsForCompressibility(item, u.asInstanceOf[ColumnType[Any, _]])
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = algo.compress(b, u)
    if (shouldNotCompress) {
      assert(algo.compressionRatio >= 1.0)
      info("CompressionRatio " + algo.compressionRatio)
    } else {

      val iter = new TestIterator(compressedBuffer, compressedBuffer.getInt())

      val oi: ObjectInspector = u match {
        case BOOLEAN => PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
        case BYTE    => PrimitiveObjectInspectorFactory.writableByteObjectInspector
        case SHORT   => PrimitiveObjectInspectorFactory.writableShortObjectInspector
        case INT     => PrimitiveObjectInspectorFactory.writableIntObjectInspector
        case LONG    => PrimitiveObjectInspectorFactory.writableLongObjectInspector
        case STRING  => PrimitiveObjectInspectorFactory.writableStringObjectInspector
      }

      l.foreach { x =>
        iter.next()
        assert(compareFunc(u.get(iter.current, oi), x))
        // assert(u.get(iter.current, oi) === x)
      }
      assert(false === iter.hasNext) // no extras at the end
    }
  }

  test("RLE Decompression Boolean") {
    testList(booleanList, BOOLEAN, new RLE())
  }

  test("RLE Decompression Byte") {
    testList(byteList, BYTE, new RLE())
  }

  test("RLE Decompression Short") {
    testList(shortList, SHORT, new RLE())
  }

  test("RLE Decompression Int") {
    testList(iList, INT, new RLE())
  }

  test("RLE Decompression Long") {
    testList(lList, LONG, new RLE())
  }

  test("RLE Decompression String") {
    testList(sList, STRING, new RLE(), (a: Text, b: Text) => a.hashCode == b.hashCode)
  }

  test("Dictionary Decompression Int") {
    testList(iList, INT, new DictionaryEncoding())
  }

  test("Dictionary Decompression Long") {
    testList(lList, LONG, new DictionaryEncoding())
  }

  test("Dictionary Decompression String") {
    testList(sList, STRING, new DictionaryEncoding(), (a: Text, b: Text) => a.hashCode == b.hashCode)
  }

  test("Dictionary Decompression at limit of unique values") {
    val alternating = Range(0, Short.MaxValue-1, 1).flatMap { s => List(1, s) }
    val iiList = byteList.map { i => i.toInt }
    val hugeList = List.concat(iiList, alternating, iiList)
    assert(hugeList.size === (8 + 2*(Short.MaxValue-1)))
    testList(hugeList, INT, new DictionaryEncoding())
  }

  test("Dictionary Decompression - should not compress") {
    val alternating = Range(0, Short.MaxValue-1, 1).flatMap { s => List(1, s) }
    val hugeList = List.concat(iList, alternating, iList)
    assert(hugeList.size === (8 + 2*(Short.MaxValue-1)))
    testList(hugeList, INT, new DictionaryEncoding(), (a: Int, b: Int) => a == b, true)
  }

  test("RLE - should not compress") {
    val alternating = Range(0, Short.MaxValue-1, 1).flatMap { s => List(1, s) }
    val hugeList = List.concat(iList, alternating, iList)
    assert(hugeList.size === (8 + 2*(Short.MaxValue-1)))
    testList(hugeList, INT, new RLE(), (a: Int, b: Int) => a == b, true)
  }

}

 class TestIterator(val buffer: ByteBuffer, val columnType: ColumnType[_,_])
      extends CompressedColumnIterator
