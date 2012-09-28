package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite


class CompressionSuite extends FunSuite {

  test("CompressedStringColumn compressed") {
    val builder = new CompressedStringColumn.Builder(5, 3)
    val data = Array[String]("0", "1", "2", null, "1", "1")
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    data.map(builder.add(_, oi))
    val c = builder.build.asInstanceOf[CompressedStringColumn]

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
    val builder = new CompressedStringColumn.Builder(5, 3)
    val data = Array[String]("0", "1", "2", null, "1", "3")
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    data.map(builder.add(_, oi))
    val c = builder.build.asInstanceOf[CompressedStringColumn]

    assert(c.backingColumn.isInstanceOf[Column.StringColumn])

    data.zipWithIndex.foreach { case(str, i) =>
      if (str == null) assert(c(i) == null)
      else assert(c(i).asInstanceOf[Text].compareTo(new Text(str)) == 0)
    }
  }

  test("DictionaryEncodedColumn") {
    val builder = new DictionaryEncodedColumn.Builder(10)
    val data = Array[String]("0", "1", "2", null, "1")
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    data.map(builder.add(_, oi))
    val c = builder.build.asInstanceOf[DictionaryEncodedColumn]

    // minus one to remove the null.
    assert(c.numDistinctWords == data.toSet.size - 1)

    data.zipWithIndex.foreach { case(str, i) =>
      if (str == null) assert(c(i) == null)
      else assert(c(i).asInstanceOf[Text].compareTo(new Text(str)) == 0)
    }
  }

}
