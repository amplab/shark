package shark.memstore

import java.io.{DataInput, DataOutput}
import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.io.Writable


class TableStorage(val size: Int, val cfs: Array[ColumnFormat[_]]) {

  def iterator = new TableIterator(this)
}


class TableIterator(table: TableStorage) extends Iterator[ColumnarStruct] {

  val struct = new ColumnarStruct(table.cfs.map(_.iterator))
  var position = 0

  def hasNext(): Boolean = position < table.size

  def next(): ColumnarStruct = {
    position += 1
    struct.nextRow
    struct
  }
}


class TableStorageBuilder(
  oi: StructObjectInspector, initialColumnSize: Int, builderFunc: ColumnBuilderCreateFunc.TYPE)
extends Writable {

  var numRows = 0
  val fields: JList[_ <: StructField] = oi.getAllStructFieldRefs
  val columnBuilders = Array.tabulate[Column.ColumnBuilder](fields.size) { i =>
    builderFunc(fields.get(i).getFieldObjectInspector, initialColumnSize)
  }
  var columns: Array[Column] = _

  def incrementRowCount() {
    numRows += 1
  }

  def append(id: Int, o: Object, oi: ObjectInspector) {
    columnBuilders(id).append(o, oi)
  }

  def build: TableStorage = {
    columns = columnBuilders.map(_.build)
    new TableStorage(numRows, columns.map(_.format))
  }

  def stats: TableStats = new TableStats(
    columns.map { column => column.stats match {
      case stats: ColumnNoStats[_] => None
      case stats: ColumnStats[_] => Some(stats)
    }}, numRows)

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) {}
  override def readFields(in: DataInput) {}
}
