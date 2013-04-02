package shark.execution.serialization

import java.io._
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.NullWritable

class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {
  def value = if (t == null) NullWritable.get else t

  override def toString = t.toString

  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new JobConf())
    ow.readFields(in)
    val s = ow.get
    if (s != null) {
      t = s.asInstanceOf[T]
    }
  }

  override def hashCode(): Int = value.hashCode
  
  override def equals(other: Any) = {
    if(other.isInstanceOf[SerializableWritable[_]].unary_!) {
      false
    } else {
      val other_t = other.asInstanceOf[SerializableWritable[_]].t
      if (t == null) {
        if (other_t == null) {
          true
        } else {
          false
        }
      } else {
        t.equals(other_t)
      }
    }
  }
}
