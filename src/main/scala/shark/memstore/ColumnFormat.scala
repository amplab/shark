package shark.memstore

import javaewah.{EWAHCompressedBitmap, IntIterator}


trait ColumnFormat[T] {

  def size: Int

  def append(v: T)

  def appendNull()

  def build: ColumnFormat[T]

  def iterator(): ColumnFormatIterator
}


trait ColumnFormatIterator {

  def nextRow()

  def current(): Object
}


trait NullBitmapColumnFormat[T] extends ColumnFormat[T] {
  def nulls: EWAHCompressedBitmap
}


abstract class NullBitmapColumnIterator[T](cf: NullBitmapColumnFormat[T])
  extends ColumnFormatIterator {

  private var _position = -1
  private var _nullsIter: IntIterator = cf.nulls.intIterator
  private var _nextNullPosition = -1

  override def nextRow() {
    _position += 1
    if (_position > _nextNullPosition) {
      _nextNullPosition = if (_nullsIter.hasNext) _nullsIter.next else Int.MaxValue
    }
  }

  override def current(): Object = {
    if (_nextNullPosition > _position) {
      getObject(_position)
    } else {
      null
    }
  }

  def getObject(i: Int): Object
}
