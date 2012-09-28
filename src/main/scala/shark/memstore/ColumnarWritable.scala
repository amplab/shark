package shark.memstore

import java.io.{DataInput, DataOutput}
import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{StructField, StructObjectInspector}
import org.apache.hadoop.io.Writable


class ColumnarWritable(
    oi: StructObjectInspector,
    initialColumnSize: Int,
    columnFactory: ColumnFactory)
  extends Writable {

  val fields: JList[_ <: StructField] = oi.getAllStructFieldRefs
  val columns = Array.tabulate[Column](fields.size) { i =>
    columnFactory.create(fields.get(i).getFieldObjectInspector, initialColumnSize)
  }

  def getField(id: Int, rowId: Int): Object = {
    columns(id)(rowId)
  }

  def add(id: Int, o: Object, oi: ObjectInspector) {
    columns(id).add(o, oi)
  }

  def close = columns.foreach { c => c.close }

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) {}
  override def readFields(in: DataInput) {}

}
