package shark.memstore

import java.io.{DataInput, DataOutput}
import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{StructField, StructObjectInspector}
import org.apache.hadoop.io.Writable


class ColumnarWritable(oi: StructObjectInspector, val columns: Array[Column]) extends Writable {

  def getField(id: Int, rowId: Int): Object = {
    columns(id)(rowId)
  }

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) = ()
  override def readFields(in: DataInput) = ()
}


object ColumnarWritable {

  class Builder(
    oi: StructObjectInspector, initialColumnSize: Int, columnFactory: ColumnBuilderFactory)
  extends Writable {

    val fields: JList[_ <: StructField] = oi.getAllStructFieldRefs
    val columns = Array.tabulate[ColumnBuilder](fields.size) { i =>
      columnFactory.createBuilder(fields.get(i).getFieldObjectInspector, initialColumnSize)
    }

    def add(id: Int, o: Object, oi: ObjectInspector) {
      columns(id).add(o, oi)
    }

    def build: ColumnarWritable = new ColumnarWritable(oi, columns.map(_.build))

    // We don't use these, but want to maintain Writable interface for SerDe
    override def write(out: DataOutput) = ()
    override def readFields(in: DataInput) = ()
  }
}
