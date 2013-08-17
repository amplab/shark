package shark.memstore2.column

import java.nio.ByteBuffer
import org.apache.hadoop.hive.serde2.`lazy`.LazyObject
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.serialization.KryoSerializer


class IntColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, INT)

class FloatColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, FLOAT)

class LongColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, LONG)

class DoubleColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, DOUBLE)

class BooleanColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, BOOLEAN)

class ByteColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, BYTE)

class ShortColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, SHORT)

class NullColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, VOID)

class TimestampColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, TIMESTAMP)

class BinaryColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, BINARY)

class StringColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, STRING)

class GenericColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, GENERIC) {
 
  private var _obj: LazyObject[_] = _
  
  override def init() {
    super.init()
    val oiSize = buffer.getInt()
    val oiSerialized = new Array[Byte](oiSize)
    buffer.get(oiSerialized, 0, oiSize)
    val oi = KryoSerializer.deserialize[ObjectInspector](oiSerialized)
    _obj = LazyFactory.createLazyObject(oi)
  }
  
  override def current() = {
    val v = super.current.asInstanceOf[ByteArrayRef]
    _obj.init(v, 0, v.getData().length)
    _obj
  }
}

class VoidColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, VOID)
