package shark.memstore2.column

import java.nio.ByteBuffer
import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.execution.serialization.KryoSerializer
import shark.memstore2.column.ColumnStats._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import java.sql.Timestamp

class GenericColumnBuilder(oi: ObjectInspector) 
  extends DefaultColumnBuilder[ByteStream.Output](new NoOpStats(), GENERIC) {
  override def initialize(numRows: Int):ByteBuffer = {
    val buffer = super.initialize(numRows)
    val objectInspectorSerialized = KryoSerializer.serialize(oi)
    buffer.putInt(objectInspectorSerialized.size)
    buffer.put(objectInspectorSerialized)
    buffer
  }
}

class BooleanColumnBuilder extends DefaultColumnBuilder[Boolean](new BooleanColumnStats(), BOOLEAN)
class IntColumnBuilder extends DefaultColumnBuilder[Int](new IntColumnStats(), INT)
class LongColumnBuilder extends DefaultColumnBuilder[Long](new LongColumnStats(), LONG)
class FloatColumnBuilder extends DefaultColumnBuilder[Float](new FloatColumnStats(), FLOAT)
class DoubleColumnBuilder extends DefaultColumnBuilder[Double](new DoubleColumnStats(), DOUBLE)
class StringColumnBuilder extends DefaultColumnBuilder[Text](new StringColumnStats(), STRING)
class ByteColumnBuilder extends DefaultColumnBuilder[Byte](new ByteColumnStats(), BYTE)
class ShortColumnBuilder extends DefaultColumnBuilder[Short](new ShortColumnStats(), SHORT)
class TimestampColumnBuilder extends DefaultColumnBuilder[Timestamp](new TimestampColumnStats(), TIMESTAMP)
class BinaryColumnBuilder extends DefaultColumnBuilder[BytesWritable](new NoOpStats(), BINARY)
class VoidColumnBuilder extends DefaultColumnBuilder[Void](new NoOpStats(), VOID)
