package shark.memstore

import java.util.{List => JList, ArrayList => JArrayList}


class ColumnarStruct(columnIterators: Array[ColumnFormatIterator]) {

  def nextRow() {
    columnIterators.foreach(_.nextRow)
  }

  def getField(id: Int): Object = columnIterators(id).current

  def getFieldsAsList(): JList[Object] = {
    val list = new JArrayList[Object](columnIterators.length)
    var i = 0
    while (i < columnIterators.length) {
      list.add(columnIterators(i).current)
      i += 1
    }
    list
  }
}
