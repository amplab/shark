package shark.memstore2.column

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import org.apache.hadoop.io.IntWritable
import shark.memstore2.column.Implicits._
import scala.collection.mutable.ArrayBuffer
import scala.math._

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
    val b = c.build
    val i = ColumnIterator.newIterator(b)
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
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    val c = ColumnBuilder.create(oi)
    c.initialize(4)
    c.append(new Text("a"), oi)
    c.append(new Text(""), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(new Text("Abcdz"), oi)
    c.append(null, oi)
    val b = c.build
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
  }
  
  test("Iterate Ints") {
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    val c = ColumnBuilder.create(oi)
    c.initialize(4)
    c.append(123.asInstanceOf[Object],oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(56.asInstanceOf[Object], oi)
    val b = c.build
    val i = ColumnIterator.newIterator(b)
    i.next()
    assert(i.current.asInstanceOf[IntWritable].get() == 123)
    i.next()
    assert(i.current == null)
    i.next()
    assert(i.current == null)
  }
  
  test("Iterate Ints RLE") {
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    val c = ColumnBuilder.create(oi)
    c.initialize(4)
    def toappend(i: Int): Int = i/10000
    Range(0,1000000).foreach { i => 
      val v = toappend(i)
      c.append(v.asInstanceOf[Object], oi)
    }
    val b = c.build
    val iter = ColumnIterator.newIterator(b)
    Range(0, 1000000).foreach { i => 
      val v = toappend(i)
      iter.next()
      val w = iter.current().asInstanceOf[IntWritable]
      assert(v == w.get())
      
    }
  }
}