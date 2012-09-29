package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BooleanObjectInspector,
  ByteObjectInspector, ShortObjectInspector, IntObjectInspector, LongObjectInspector,
  FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.Text


sealed trait ColumnWithStats[T] extends ColumnBuilder {
  val stats: ColumnStats[T]
}

object ColumnWithStats extends ColumnBuilderFactory {

  override def createBuilder(oi: ObjectInspector, initialSize: Int): ColumnBuilder = {
    oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN => new BooleanColumnBuilder(initialSize)
          case PrimitiveCategory.BYTE => new ByteColumnBuilder(initialSize)
          case PrimitiveCategory.SHORT => new ShortColumnBuilder(initialSize)
          case PrimitiveCategory.INT => new IntColumnBuilder(initialSize)
          case PrimitiveCategory.LONG => new LongColumnBuilder(initialSize)
          case PrimitiveCategory.FLOAT => new FloatColumnBuilder(initialSize)
          case PrimitiveCategory.DOUBLE => new DoubleColumnBuilder(initialSize)
          case PrimitiveCategory.STRING => new StringColumnBuilder(initialSize)
          case PrimitiveCategory.VOID => new Column.VoidColumn.Builder()
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new Column.LazyColumn.Builder(oi, initialSize)
    }
  }

  class BooleanColumnBuilder(initialSize: Int) extends ColumnWithStats[Boolean] {
    val column = new Column.BooleanColumn.Builder(initialSize)
    override val stats = new ColumnStats.BooleanColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[BooleanObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class ByteColumnBuilder(initialSize: Int) extends ColumnWithStats[Byte] {
    val column = new Column.ByteColumn.Builder(initialSize)
    override val stats = new ColumnStats.ByteColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[ByteObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class ShortColumnBuilder(initialSize: Int) extends ColumnWithStats[Short] {
    val column = new Column.ShortColumn.Builder(initialSize)
    override val stats = new ColumnStats.ShortColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[ShortObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class IntColumnBuilder(initialSize: Int) extends ColumnWithStats[Int] {
    val column = new Column.IntColumn.Builder(initialSize)
    override val stats = new ColumnStats.IntColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[IntObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class LongColumnBuilder(initialSize: Int) extends ColumnWithStats[Long] {
    val column = new Column.LongColumn.Builder(initialSize)
    override val stats = new ColumnStats.LongColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[LongObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class FloatColumnBuilder(initialSize: Int) extends ColumnWithStats[Float] {
    val column = new Column.FloatColumn.Builder(initialSize)
    override val stats = new ColumnStats.FloatColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[FloatObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class DoubleColumnBuilder(initialSize: Int) extends ColumnWithStats[Double] {
    val column = new Column.DoubleColumn.Builder(initialSize)
    override val stats = new ColumnStats.DoubleColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[DoubleObjectInspector].get(o))
      column.add(o, oi)
    }
  }

  class StringColumnBuilder(initialSize: Int) extends ColumnWithStats[Text] {
    val column = new CompressedStringColumn.Builder(initialSize, 32)
    override val stats = new ColumnStats.TextColumnStats
    override def build: Column = column.build

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o))
      column.add(o, oi)
    }
  }
}
