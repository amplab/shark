package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.scalatest.FunSuite

import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.memstore2.column.Implicits._

class CompressedColumnIteratorSuite extends FunSuite {

  /**
   * Generic tester across types and encodings
   */
  def testList[T, W](
      l: Seq[T],
      t: ColumnType[T, _],
      algo: CompressionAlgorithm,
      compareFunc: (T, T) => Boolean = (a: T, b: T) => a == b,
      shouldNotCompress: Boolean = false)
  {
    val b = ByteBuffer.allocate(1024 + (3 * 40 * l.size))
    b.order(ByteOrder.nativeOrder())
    b.putInt(t.typeID)
    l.foreach { item =>
      t.append(item, b)
      algo.gatherStatsForCompressibility(item, t.asInstanceOf[ColumnType[Any, _]])
    }
    b.limit(b.position())
    b.rewind()

    if (shouldNotCompress) {
      assert(algo.compressionRatio >= 1.0)
    } else {
      val compressedBuffer = algo.compress(b, t)
      val iter = new TestIterator(compressedBuffer, compressedBuffer.getInt())

      val oi: ObjectInspector = t match {
        case BOOLEAN => PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
        case BYTE    => PrimitiveObjectInspectorFactory.writableByteObjectInspector
        case SHORT   => PrimitiveObjectInspectorFactory.writableShortObjectInspector
        case INT     => PrimitiveObjectInspectorFactory.writableIntObjectInspector
        case LONG    => PrimitiveObjectInspectorFactory.writableLongObjectInspector
        case STRING  => PrimitiveObjectInspectorFactory.writableStringObjectInspector
        case _       => throw new UnsupportedOperationException("Unsupported compression type " + t)
      }

      l.foreach { x =>
        iter.next()
        assert(compareFunc(t.get(iter.current, oi), x))
      }

      // Make sure we reach the end of the iterator.
      assert(!iter.hasNext)
    }
  }

  test("RLE Boolean") {
    testList(Seq[Boolean](true, true, false, true), BOOLEAN, new RLE())
  }

  test("RLE Byte") {
    testList(Seq[Byte](10, 10, 20, 10), BYTE, new RLE())
  }

  test("RLE Short") {
    testList(Seq[Short](10, 10, 10, 20000, 20000, 20000, 500, 500, 500, 500), SHORT, new RLE())
  }

  test("RLE Int") {
    testList(Seq[Int](1000000, 1000000, 1000000, 1000000, 900000, 99), INT, new RLE())
  }

  test("RLE Long") {
    val longs = Seq[Long](2147483649L, 2147483649L, 2147483649L, 2147483649L, 500L, 500L, 500L)
    testList(longs, LONG, new RLE())
  }

  test("RLE String") {
    val strs: Seq[Text] = Seq("abcd", "abcd", "abcd", "e", "e", "!", "!").map(s => new Text(s))
    testList(strs, STRING, new RLE(), (a: Text, b: Text) => a.equals(b))
  }

  test("Dictionary Encoded Int") {
    testList(Seq[Int](1000000, 1000000, 1000000, 1000000, 900000, 99), INT, new DictionaryEncoding)
  }

  test("Dictionary Encoded Long") {
    val longs = Seq[Long](2147483649L, 2147483649L, 2147483649L, 2147483649L, 500L, 500L, 500L)
    testList(longs, LONG, new DictionaryEncoding)
  }

  test("Dictionary Encoded String") {
    val strs: Seq[Text] = Seq("abcd", "abcd", "abcd", "e", "e", "!", "!").map(s => new Text(s))
    testList(strs, STRING, new DictionaryEncoding, (a: Text, b: Text) => a.equals(b),
      shouldNotCompress = false)
  }

  test("Dictionary Encoding at limit of unique values") {
    val ints = Range(0, Short.MaxValue - 1).flatMap(i => Iterator(i, i, i))
    testList(ints, INT, new DictionaryEncoding)
  }

  test("Dictionary Encoding - should not compress") {
    val ints = Range(0, Short.MaxValue.toInt)
    testList(ints, INT, new DictionaryEncoding, (a: Int, b: Int) => a == b,
      shouldNotCompress = true)
  }

  test("RLE - should not compress") {
    val ints = Range(0, Short.MaxValue.toInt + 1)
    testList(ints, INT, new RLE, (a: Int, b: Int) => a == b, shouldNotCompress = true)
  }
}


class TestIterator(val buffer: ByteBuffer, val columnType: ColumnType[_,_])
    extends CompressedColumnIterator
