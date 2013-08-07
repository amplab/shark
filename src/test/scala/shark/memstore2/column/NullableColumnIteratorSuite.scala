package shark.memstore2.column

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import org.apache.hadoop.io.IntWritable


class NullableColumnIteratorSuite extends FunSuite {

  test("String Growth") {
    val c = new StringColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    val a = Array[Text](
        new Text("a"), null,
        new Text("b"), null,
        new Text("abc"), null,
        null, null, new Text("efg")
    )
    a.foreach {
      t => c.append(t, oi)
    }
    val b = c.build
    val columnType = b.getInt()
    val i = ColumnIterator.newIterator(columnType, b)
    Range(0, a.length).foreach { x =>
      i.next()
      val v = i.current
      if (a(x) == null) {
        assert(v == null)
      } else {
        assert(v.toString == a(x).toString)
      }
    }
  }
  test("Iterate Strings") {
    val c = new StringColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    c.append(new Text("a"), oi)
    c.append(new Text(""), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(new Text("Abcdz"), oi)
    c.append(null, oi)
    val b = c.build
    val columnType = b.getInt()
    val i = ColumnIterator.newIterator(columnType, b)
    i.next()
    assert(i.current.toString() == "a")
    i.next()
    assert(i.current.toString() == "")
    i.next()
    assert(i.current == null)
    i.next()
    assert(i.current.toString() == "b")
    i.next()
    assert(i.current.toString() == "Abcdz")
    i.next()
    assert(i.current == null)
  }
  
  test("Iterate Ints") {
    val c = new IntColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    c.append(123.asInstanceOf[Object],oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(56.asInstanceOf[Object], oi)
    val b = c.build
    val columnType = b.getInt()
    val i = ColumnIterator.newIterator(columnType, b)
    i.next()
    assert(i.current.asInstanceOf[IntWritable].get() == 123)
    i.next()
    assert(i.current == null)
    i.next()
    assert(i.current == null)
  }
}