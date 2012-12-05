package shark.execution.serialization

import java.nio.ByteBuffer


object JavaSerializer {
  @transient val ser = new spark.JavaSerializer

  def serialize[T](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}
