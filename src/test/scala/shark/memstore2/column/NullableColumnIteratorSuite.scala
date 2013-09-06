package shark.memstore2.column

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

import org.scalatest.FunSuite


class NullableColumnIteratorSuite extends FunSuite {

  test("String Growth") {
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    val c = ColumnBuilder.create(oi)
    c.initialize(4)
    val a = Array[Text](
        new Text("a"), null,
        new Text("b"), null,
        new Text("abc"), null,
        null, null, new Text("efg")
    )
    a.foreach {
      t => c.append(t, oi)
    }
    val b = c.build()
    val i = ColumnIterator.newIterator(b)
    Range(0, a.length).foreach { x =>
      if (x > 0) assert(i.hasNext)
      i.next()
      val v = i.current
      if (a(x) == null) {
        assert(v == null)
      } else {
        assert(v.toString == a(x).toString)
      }
    }
    assert(!i.hasNext)
  }

  test("Iterate Strings") {
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    val c = ColumnBuilder.create(oi)
    c.initialize(4)
    c.append(new Text("a"), oi)
    c.append(new Text(""), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(new Text("Abcdz"), oi)
    c.append(null, oi)
    val b = c.build()
    val i = ColumnIterator.newIterator(b)
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
    assert(false === i.hasNext)
  }
  
  test("Iterate Ints") {
    def testList(l: Seq[AnyRef]) {
      val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
      val c = ColumnBuilder.create(oi)
      c.initialize(l.size)

      l.foreach { item =>
        c.append(item, oi)
      }

      val b = c.build()
      val i = ColumnIterator.newIterator(b)

      l.foreach { x =>
        i.next()
        if (x == null) {
          assert(i.current === x)
        } else {
          assert(i.current.asInstanceOf[IntWritable].get === x)
        }
      }
      assert(false === i.hasNext)
    }

    testList(List(null, null, 123.asInstanceOf[AnyRef]))
    testList(List(123.asInstanceOf[AnyRef], 4.asInstanceOf[AnyRef], null))
    testList(List(null))
  }
}
