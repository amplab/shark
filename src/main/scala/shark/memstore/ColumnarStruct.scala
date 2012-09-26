package shark.memstore

import java.util.{List => JList, ArrayList => JArrayList}


class ColumnarStruct(val data: ColumnarWritable) {

  var row = -1

  def initializeNextRow() {
    row += 1
  }

  def getField(id: Int): Object = data.getField(id, row)

  def getFieldsAsList(): JList[Object] = {
    val list = new JArrayList[Object](data.columns.length)
    var i = 0
    while (i < data.columns.length) {
      list.add(data.getField(i, row))
      i += 1
    }
    list
  }
}
