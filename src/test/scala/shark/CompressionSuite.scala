package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

class CompressionSuite extends FunSuite {

  test("CompressedStringColumn compressed") {
    val c = new CompressedStringColumn(5, 3)
    val data = Array[String]("0", "1", "2", null, "1")
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    data.map(c.add(_, oi))
    c.close

    assert(c.isCompressed)
    assert(c.backingColumn.isInstanceOf[DictionaryEncodedColumn])
    val backingColumn = c.backingColumn.asInstanceOf[DictionaryEncodedColumn]

    // minus one to remove the null.
    assert(backingColumn.numDistinctWords == data.toSet.size - 1)

    data.zipWithIndex.foreach { case(str, i) =>
      if (str == null) assert(c(i) == null)
      else assert(c(i).asInstanceOf[Text].compareTo(new Text(str)) == 0)
    }
  }

  test("CompressedStringColumn uncompressed") {
    val c = new CompressedStringColumn(5, 3)
    val data = Array[String]("0", "1", "2", null, "1", "3")
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    data.map(c.add(_, oi))
    c.close

    assert(!c.isCompressed)
    assert(c.backingColumn.isInstanceOf[Column.StringColumn])

    data.zipWithIndex.foreach { case(str, i) =>
      if (str == null) assert(c(i) == null)
      else assert(c(i).asInstanceOf[Text].compareTo(new Text(str)) == 0)
    }
  }

  test("DictionaryEncodedColumn") {
    val c = new DictionaryEncodedColumn(10)
    val data = Array[String]("0", "1", "2", null, "1")
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    data.map(c.add(_, oi))
    c.close

    // minus one to remove the null.
    assert(c.numDistinctWords == data.toSet.size - 1)

    data.zipWithIndex.foreach { case(str, i) =>
      if (str == null) assert(c(i) == null)
      else assert(c(i).asInstanceOf[Text].compareTo(new Text(str)) == 0)
    }
  }

}
