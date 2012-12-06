/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.io.Text

import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap
import it.unimi.dsi.fastutil.shorts.ShortArrayList


/** A string column that optionally compresses data. This data structure initially
 * attempts to use dictionary encoding to compress the data. If there are too many
 * unique words, the class abandons compression and uses a normal string column.
 */
class CompressedTextColumnFormat(
  initialSize: Int, maxDistinctWords: Int) extends ColumnFormat[Text] {

  import CompressedTextColumnFormat.DictionaryEncodedColumnFormat

  private var _isCompressed = true
  private var _column: ColumnFormat[Text] = new DictionaryEncodedColumnFormat(initialSize)

  def backingColumn = _column

  override def size: Int = _column.size

  override def append(v: Text) {
    _column.append(v)

    if (_isCompressed &&
        _column.asInstanceOf[DictionaryEncodedColumnFormat].numDistinctWords > maxDistinctWords) {
      // Should turn compression off since there are too many distinct words.
      val compressedColumn = _column.build.asInstanceOf[DictionaryEncodedColumnFormat]
      val uncompressedColumn = new UncompressedColumnFormat.TextColumnFormat(
        math.max(initialSize, compressedColumn.size))
      var i = 0
      while (i < compressedColumn.size) {
        val v = compressedColumn.getObject(i)
        if (v == null) uncompressedColumn.appendNull()
        else uncompressedColumn.append(v.asInstanceOf[Text])
        i += 1
      }
      _column = uncompressedColumn
      _isCompressed = false
    }
  }

  override def appendNull() {
    _column.appendNull()
  }

  override def build = {
    _column.build
    this
  }

  override def iterator: ColumnFormatIterator = _column.iterator
}


object CompressedTextColumnFormat {

  class DictionaryEncodedColumnFormat(initialSize: Int) extends ColumnFormat[Text] {

    override def size: Int = _data.size

    override def append(value: Text) {
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

    override def appendNull() {
      _data.add(NULL_VALUE)
    }

    override def build: ColumnFormat[Text] = {
      _encodingMap = null
      _data.trim
      this
    }

    def getObject(i: Int) = _dictionary(_data.getShort(i))

    override def iterator: ColumnFormatIterator = new ColumnFormatIterator {
      var _position = -1
      override def nextRow() { _position += 1 }
      override def current: Object = getObject(_position)
    }

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
  }
}
