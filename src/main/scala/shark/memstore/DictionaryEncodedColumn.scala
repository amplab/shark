package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.io.Text

import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap
import it.unimi.dsi.fastutil.shorts.ShortArrayList

import scala.collection.mutable.ArrayBuffer


/** A string column that optionally compresses data. This data structure initially
 * attempts to use dictionary encoding to compress the data. If there are too many
 * unique words, the class abandons compression and uses a normal string column.
 */
class CompressedStringColumn(initialSize: Int, maxDistinctWords: Int) extends Column {

  override def apply(i: Int): Object = _column(i)
  override def close: Unit = _column.close
  override def size: Int = _column.size

  override def add(o: Object, oi: ObjectInspector) {
    _column.add(o, oi)

    if (_isCompressed &&
        _column.asInstanceOf[DictionaryEncodedColumn].numDistinctWords > maxDistinctWords) {
      // Should turn compression off since there are too many distinct words.
      val uncompressedColumn = new Column.StringColumn(math.max(initialSize, size))
      var i = 0
      while (i < size) {
        uncompressedColumn.add(
          this(i), PrimitiveObjectInspectorFactory.writableStringObjectInspector)
        i += 1
      }
      _column = uncompressedColumn
      _isCompressed = false
    }
  }

  def isCompressed: Boolean = _isCompressed

  def backingColumn: Column = _column

  private var _isCompressed = true
  private var _column: Column = new DictionaryEncodedColumn(initialSize)
}


class DictionaryEncodedColumn(initialSize: Int) extends Column {
  // The value in "data" used to describe null.
  val NULL_VALUE = 0.toShort

  // The number of distinct words in the dictionary. The count doesn't include null.
  private var _numDistinctWords: Int = 0
  def numDistinctWords = _numDistinctWords

  // Map from object to the compressed value (short). Compressed values should
  // start from 1, since 0 is used to indicate null.
  private var _encodingMap = new Object2ShortOpenHashMap[Text]()

  // The dictionary lookup maps a short value to the object it represents.
  private var _dictionary = ArrayBuffer[Text]()
  _dictionary += null

  // The list of compressed values.
  private var _data = new ShortArrayList(initialSize)

  override def apply(i: Int): Text = _dictionary(_data.getShort(i))

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      _data.add(NULL_VALUE)
    } else {
      val value: Text = oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
      var encodedValue: Short = _encodingMap.getShort(value)
      if (encodedValue == 0) {
        // A new word. Add it to the dictionary. We make a copy of the Text
        // just in case since Text is mutable.
        val clonedValue = new Text(value)
        _numDistinctWords += 1
        encodedValue = _numDistinctWords.toShort
        _encodingMap.put(clonedValue, encodedValue)
        _dictionary += clonedValue
      }
      _data.add(encodedValue)
    }
  }

  override def close {
    _data.trim
    _encodingMap = null
  }

  override def size: Int = _data.size
}
