package shark.memstore

import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BooleanObjectInspector,
  ByteObjectInspector, ShortObjectInspector, IntObjectInspector, LongObjectInspector,
  FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.Text


/**
 * A immutable column of data. These columns provide methods to access value at
 * a specific position and returns Hadoop writable objects. A column object
 * should be built by a ColumnBuilder.
 */
class Column(val format: ColumnFormat[_], val stats: ColumnStats[_]) {
  def apply(i: Int): Object = format(i)
  def size: Int = format.size
}

object Column {

  /**
   * An append-only, mutable data structure used to build a Column.
   */
  sealed trait ColumnBuilder {
    def append(o: Object, oi: ObjectInspector)
    def build: Column
  }

  class BooleanColumnBuilder(val format: ColumnFormat[Boolean], val stats: ColumnStats[Boolean])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[BooleanObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class ByteColumnBuilder(val format: ColumnFormat[Byte], val stats: ColumnStats[Byte])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[ByteObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class ShortColumnBuilder(val format: ColumnFormat[Short], val stats: ColumnStats[Short])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[ShortObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class IntColumnBuilder(val format: ColumnFormat[Int], val stats: ColumnStats[Int])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[IntObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class LongColumnBuilder(val format: ColumnFormat[Long], val stats: ColumnStats[Long])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[LongObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class FloatColumnBuilder(val format: ColumnFormat[Float], val stats: ColumnStats[Float])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[FloatObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class DoubleColumnBuilder(val format: ColumnFormat[Double], val stats: ColumnStats[Double])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[DoubleObjectInspector].get(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class TextColumnBuilder(val format: ColumnFormat[Text], val stats: ColumnStats[Text])
    extends ColumnBuilder {

    override def append(o: Object, oi: ObjectInspector) {
      if (o == null) {
        format.appendNull()
        stats.appendNull()
      } else {
        val v = oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
        format.append(v)
        stats.append(v)
      }
    }

    override def build: Column = new Column(format.build, stats.build)
  }

  class VoidColumnBuilder extends ColumnBuilder {
    override def append(o: Object, oi: ObjectInspector) {}
    override def build: Column = new Column(
      new UncompressedColumnFormat.VoidColumnFormat, ColumnStats.GenericColumnNoStats)
  }

  class LazyColumnBuilder(outputOI: ObjectInspector, initialSize: Int) extends ColumnBuilder {
    val format = new UncompressedColumnFormat.LazyColumnFormat(outputOI, initialSize)

    override def append(o: Object, oi: ObjectInspector) {
      val bytes = o.asInstanceOf[ByteStream.Output]
      format.append(bytes)
    }

    override def build = new Column(format, ColumnStats.GenericColumnNoStats)
  }
}


