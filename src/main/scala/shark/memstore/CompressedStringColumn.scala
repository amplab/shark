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
class CompressedStringColumn(val backingColumn: Column) extends Column {
  override def apply(i: Int): Object = backingColumn(i)
  override def size: Int = backingColumn.size
}


object CompressedStringColumn {

  class Builder(initialSize: Int, maxDistinctWords: Int) extends ColumnBuilder {

    type CompressedColumnBuilder = DictionaryEncodedColumn.Builder

    private var _isCompressed = true
    private var _column: ColumnBuilder =
      new CompressedColumnBuilder(initialSize)

    override def add(o: Object, oi: ObjectInspector) {
      _column.add(o, oi)

      if (_isCompressed &&
          _column.asInstanceOf[CompressedColumnBuilder].numDistinctWords > maxDistinctWords) {
        // Should turn compression off since there are too many distinct words.
        val compressedColumn = _column.build
        val uncompressedColumn = new Column.StringColumn.Builder(
          math.max(initialSize, compressedColumn.size))
        var i = 0
        while (i < compressedColumn.size) {
          uncompressedColumn.add(
            compressedColumn(i), PrimitiveObjectInspectorFactory.writableStringObjectInspector)
          i += 1
        }
        _column = uncompressedColumn
        _isCompressed = false
      }
    }

    override def build: Column = new CompressedStringColumn(_column.build)
  }
}


class DictionaryEncodedColumn(
  private val _dictionary: Array[Text],
  private val _data: Array[Short])
extends Column {

  override def apply(i: Int): Text = _dictionary(_data(i))
  override def size: Int = _data.size

  def numDistinctWords = _dictionary.size - 1
}

object DictionaryEncodedColumn {
  // The value in "data" used to describe null.
  val NULL_VALUE = 0.toShort

  class Builder(initialSize: Int) extends ColumnBuilder {

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

    override def build: Column = {
      _encodingMap = null
      _data.trim
      new DictionaryEncodedColumn(_dictionary.toArray, _data.elements)
    }
  }
}
