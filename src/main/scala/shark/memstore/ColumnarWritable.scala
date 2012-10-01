package shark.memstore

import java.io.{DataInput, DataOutput}
import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{StructField, StructObjectInspector}
import org.apache.hadoop.io.Writable


class ColumnarWritable private (
  oi: StructObjectInspector, val columns: Array[Column]) extends Writable {

  def getField(id: Int, rowId: Int): Object = columns(id)(rowId)

  def stats: TableStats = new TableStats(
    columns.map { column => column.stats match {
      case stats: ColumnNoStats[_] => None
      case stats: ColumnStats[_] => Some(stats)
    }})

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) = ()
  override def readFields(in: DataInput) = ()
}


object ColumnarWritable {

  class Builder(
    oi: StructObjectInspector, initialColumnSize: Int, builderFunc: ColumnBuilderCreateFunc.TYPE)
  extends Writable {

    val fields: JList[_ <: StructField] = oi.getAllStructFieldRefs
    val columns = Array.tabulate[Column.ColumnBuilder](fields.size) { i =>
      builderFunc(fields.get(i).getFieldObjectInspector, initialColumnSize)
    }

    def append(id: Int, o: Object, oi: ObjectInspector) {
      columns(id).append(o, oi)
    }

    def build: ColumnarWritable = new ColumnarWritable(oi, columns.map(_.build))

    // We don't use these, but want to maintain Writable interface for SerDe
    override def write(out: DataOutput) = ()
    override def readFields(in: DataInput) = ()
  }
}
