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

package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.StructField

import shark.LogHelper


trait ColumnBuilder[T] {

  private[column] def t: ColumnType[T, _]

  private[memstore2] def stats: ColumnStats[T]

  private var _buffer: ByteBuffer = _
  private var _initialSize: Int = _
  private var _columnName: String = _

  def columnName: String = _columnName

  def append(o: Object, oi: ObjectInspector) {
    val v = t.get(o, oi)
    _buffer = growIfNeeded(_buffer, t.actualSize(v))
    t.append(v, _buffer)
    gatherStats(v)
  }

  protected def gatherStats(v: T) {
    stats.append(v)
  }

  def build(): ByteBuffer = {
    _buffer.limit(_buffer.position())
    _buffer.rewind()
    _buffer
  }

  /**
   * Initialize with an approximate lower bound on the expected number
   * of elements in this column.
   */
  def initialize(initialSize: Int, colName: String = ""): ByteBuffer = {
    _columnName = colName
    // Use 10MB as the initial size for the column if it is not specified.
    _initialSize = if (initialSize == 0) 1024 * 1024 * 10 else initialSize
    _buffer = ByteBuffer.allocate(_initialSize * t.defaultSize + 4 + 4)
    _buffer.order(ByteOrder.nativeOrder())
    _buffer.putInt(t.typeID)
  }

  protected def growIfNeeded(orig: ByteBuffer, size: Int): ByteBuffer = {
    val capacity = orig.capacity()
    if (orig.remaining() < size) {
      // grow in steps of initial size
      val additionalSize = capacity / 8 + 1
      var newSize = capacity + additionalSize
      if (additionalSize  < size) {
        newSize = capacity + size
      }
      val pos = orig.position()
      orig.clear()
      val b = ByteBuffer.allocate(newSize)
      b.order(ByteOrder.nativeOrder())
      b.put(orig.array(), 0, pos)
    } else {
      orig
    }
  }
}

class DefaultColumnBuilder[T](val stats: ColumnStats[T], val t: ColumnType[T, _])
  extends CompressedColumnBuilder[T] with NullableColumnBuilder[T]


trait CompressedColumnBuilder[T] extends ColumnBuilder[T] with LogHelper {

  var compressionSchemes: Seq[CompressionAlgorithm] = Seq()
  // Can be set in tests to ensure chosen compression
  var scheme: CompressionAlgorithm = new NoCompression

  def shouldApply(scheme: CompressionAlgorithm): Boolean = {
    scheme.compressionRatio < 0.8
  }

  override protected def gatherStats(v: T) = {
    compressionSchemes.foreach { scheme =>
      if (scheme.supportsType(t)) {
        scheme.gatherStatsForCompressibility(v, t)
      }
    }
    super.gatherStats(v)
  }

  override def build() = {
    val b = super.build()

    import shark.memstore2.column.Implicits._

    if (compressionSchemes.isEmpty) {
      val strType: String = scheme.compressionType
      logInfo(s"Compression scheme chosen for [$columnName] is $strType - no compression")
      new NoCompression().compress(b, t)
    } else {
      val candidateScheme = scheme.compressionType match {
        case DefaultCompressionType => compressionSchemes.minBy(_.compressionRatio)
        case _ => scheme
      }

      val strType: String = candidateScheme.compressionType
      logInfo(s"Compression scheme chosen for [$columnName] is $strType with ratio " +
        candidateScheme.compressionRatio)
      if (shouldApply(candidateScheme)) {
        candidateScheme.compress(b, t)
      } else {
        new NoCompression().compress(b, t)
      }
    }
  }
}


object ColumnBuilder {

  def create(structField: StructField, shouldCompress: Boolean = true): ColumnBuilder[_] = {
    val columnOi = structField.getFieldObjectInspector

    val v = columnOi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        columnOi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN   => new BooleanColumnBuilder
          case PrimitiveCategory.INT       => new IntColumnBuilder
          case PrimitiveCategory.LONG      => new LongColumnBuilder
          case PrimitiveCategory.FLOAT     => new FloatColumnBuilder
          case PrimitiveCategory.DOUBLE    => new DoubleColumnBuilder
          case PrimitiveCategory.STRING    => new StringColumnBuilder
          case PrimitiveCategory.SHORT     => new ShortColumnBuilder
          case PrimitiveCategory.BYTE      => new ByteColumnBuilder
          case PrimitiveCategory.TIMESTAMP => new TimestampColumnBuilder
          case PrimitiveCategory.BINARY    => new BinaryColumnBuilder

          // TODO: add decimal column.
          case _ => throw new MemoryStoreException(
            "Invalid primitive object inspector category" + columnOi.getCategory)
        }
      }
      case _ => new GenericColumnBuilder(columnOi)
    }
    if (shouldCompress) {
      v.compressionSchemes = Seq(
        new NoCompression,
        new RLE,
        new BooleanBitSetCompression,
        new DictionaryEncoding,
        new IntDeltaEncoding,
        new LongDeltaEncoding)
    }
    v
  }
}
