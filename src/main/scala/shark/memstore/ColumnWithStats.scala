package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BooleanObjectInspector,
  ByteObjectInspector, ShortObjectInspector, IntObjectInspector, LongObjectInspector,
  FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.Text


trait ColumnWithStats[T] extends Column {
  val stats: ColumnStats[T]
}


object ColumnWithStats extends ColumnFactory {

  override def create(oi: ObjectInspector, initialSize: Int): Column = {
    oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN => new BooleanColumn(initialSize)
          case PrimitiveCategory.BYTE => new ByteColumn(initialSize)
          case PrimitiveCategory.SHORT => new ShortColumn(initialSize)
          case PrimitiveCategory.INT => new IntColumn(initialSize)
          case PrimitiveCategory.LONG => new LongColumn(initialSize)
          case PrimitiveCategory.FLOAT => new FloatColumn(initialSize)
          case PrimitiveCategory.DOUBLE => new DoubleColumn(initialSize)
          case PrimitiveCategory.STRING => new StringColumn(initialSize)
          case PrimitiveCategory.VOID => new Column.VoidColumn()
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new Column.LazyColumn(oi, initialSize)
    }
  }

  class BooleanColumn(initialSize: Int) extends Column.BooleanColumn(initialSize)
    with ColumnWithStats[Boolean] {

    override val stats = new RangeStats.NumericRangeStats[Boolean]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[BooleanObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class ByteColumn(initialSize: Int) extends Column.ByteColumn(initialSize)
    with ColumnWithStats[Byte] {

    override val stats = new RangeStats.NumericRangeStats[Byte]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[ByteObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class ShortColumn(initialSize: Int) extends Column.ShortColumn(initialSize)
    with ColumnWithStats[Short] {

    val stats = new RangeStats.NumericRangeStats[Short]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[ShortObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class IntColumn(initialSize: Int) extends Column.IntColumn(initialSize)
    with ColumnWithStats[Int] {

    override val stats = new RangeStats.NumericRangeStats[Int]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[IntObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class LongColumn(initialSize: Int) extends Column.LongColumn(initialSize)
    with ColumnWithStats[Long] {

    override val stats = new RangeStats.NumericRangeStats[Long]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[LongObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class FloatColumn(initialSize: Int) extends Column.FloatColumn(initialSize)
    with ColumnWithStats[Float] {

    override val stats = new RangeStats.NumericRangeStats[Float]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[FloatObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class DoubleColumn(initialSize: Int) extends Column.DoubleColumn(initialSize)
    with ColumnWithStats[Double] {

    override val stats = new RangeStats.NumericRangeStats[Double]

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[DoubleObjectInspector].get(o))
      super.add(o, oi)
    }
  }

  class StringColumn(initialSize: Int) extends Column.StringColumn(initialSize)
    with ColumnWithStats[Text] {

    override val stats = new RangeStats.TextRangeStats

    override def add(o: Object, oi: ObjectInspector) {
      stats.add(oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o))
      super.add(o, oi)
    }
  }
}