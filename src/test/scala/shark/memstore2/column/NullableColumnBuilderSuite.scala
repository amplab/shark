package shark.memstore2.column

import org.scalatest.FunSuite
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import shark.memstore2.column.Implicits._


class NullableColumnBuilderSuite extends FunSuite {

  test("Perf") {
    val c = new StringColumnBuilder()
    c.initialize(1) //initialize to some low value
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    Range(0, 10000000).foreach { i =>
      c.append(new Text("00000000000000000000000000000000" + i), oi)
    }
    val b = c.build
    val i = ColumnIterator.newIterator(b)
    Range(0, 1000000).foreach { x =>
      i.next()
      i.current
    }
  }
  test("Grow") {
    val c = new StringColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    c.append(new Text("a"), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(null, oi)
    c.append(new Text("abc"), oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(new Text("efg"), oi)
    val b = c.build
    val colType = b.getInt()
    assert(colType == STRING.typeID)
    
  }
  test("Null Strings") {
    val c = new StringColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector
    c.append(new Text("a"), oi)
    c.append(null, oi)
    c.append(new Text("b"), oi)
    c.append(null, oi)
    val b = c.build
    //expect first element is col type
    assert(b.getInt() == STRING.typeID)
    //next comes # of nulls
    assert(b.getInt() == 2)
    //typeID of first null is 1, that of second null is 3
    assert(b.getInt() == 1)
    assert(b.getInt() == 3)
   
    //next comes the compression type
    assert(b.getInt() == -1)
    assert(b.getInt() == 1)
    assert(b.get() == 97)
    assert(b.getInt() == 1)
    assert(b.get() == 98)
  }
  
  test("Null Ints") {
    val c = new IntColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    c.append(123.asInstanceOf[Object], oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(56.asInstanceOf[Object], oi)
    val b = c.build
    //expect first element is col type
    assert(b.getInt() == INT.typeID)
    //next comes # of nulls
    assert(b.getInt() == 2)
    //typeID of first null is 1, that of second null is 3
    assert(b.getInt() == 1)
    assert(b.getInt() == 2)
    assert(b.getInt() == -1)
    assert(b.getInt() == 123)
  }
  
  test("Null Longs") {
    val c = new LongColumnBuilder()
    c.initialize(4)
    val oi = PrimitiveObjectInspectorFactory.javaLongObjectInspector
    c.append(123L.asInstanceOf[Object], oi)
    c.append(null, oi)
    c.append(null, oi)
    c.append(56L.asInstanceOf[Object], oi)
    val b = c.build
    //expect first element is col type
    assert(b.getInt() == LONG.typeID)
    //next comes # of nulls
    assert(b.getInt() == 2)
    //typeID of first null is 1, that of second null is 3
    assert(b.getInt() == 1)
    assert(b.getInt() == 2)
    assert(b.getInt() == -1)
    assert(b.getLong() == 123L)
  }
  
  test("Trigger RLE") {
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    var c = ColumnBuilder.create(oi)
    c.initialize(4)
    Range(1,1000).foreach { x =>
      c.append(x.asInstanceOf[Object], oi)
    }
    var b = c.build
    assert(b.getInt() == INT.typeID)
    //next come the nulls.
    assert(b.getInt() == 0)
    assert(b.getInt() == DEFAULT.typeID) 
    
    //now trigger RLE
    c = ColumnBuilder.create(oi)
    c.initialize(4)
    Range(0,1000).foreach { x =>
      if (x < 600)
      c.append(1.asInstanceOf[Object], oi)
      else if (x < 700) c.append(x.asInstanceOf[Object], oi)
      else c.append(2.asInstanceOf[Object], oi)
    }
    
    b = c.build
    assert(b.getInt() == INT.typeID)
    //next come the nulls.
    assert(b.getInt() == 0)
    assert(b.getInt() == RLECompressionType.typeID) 
    //expect 1, followed by a count of 600
    assert(b.getInt() == 1)
    assert(b.getInt() == 600)
   
    //skip 200
    Range(600,700).foreach {x =>
      assert(b.getInt() == x)
      assert(b.getInt() == 1)
    }
    assert(b.getInt() == 2)
    assert(b.getInt() == 300)
    
  }
}