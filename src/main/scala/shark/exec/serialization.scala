package shark.exec

import java.beans.{XMLDecoder, XMLEncoder, PersistenceDelegate}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutput, ObjectInput}

import org.apache.hadoop.hive.ql.exec.Utilities.EnumDelegate
import org.apache.hadoop.hive.ql.plan.GroupByDesc
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes

import shark.LogHelper


/**
 * A wrapper around our operators so they can be serialized by standard Java
 * serialization. This really just delegates the serialization of the operators
 * to XML, and that of object inspectors to Kryo.
 */
class OperatorSerializationWrapper[T <: Operator[_ <: HiveOperator]]
extends java.io.Externalizable with shark.LogHelper {

  var value: T = _

  override def readExternal(in: ObjectInput) {
    val start: Long = System.currentTimeMillis()
    val opSize = in.readInt()
    val objectInspectorsSize = in.readInt()
    val serializedOp = new Array[Byte](opSize)
    val serializedObjectInspectors = new Array[Byte](objectInspectorsSize)
    in.readFully(serializedOp)
    in.readFully(serializedObjectInspectors)
    value = XmlSerializer.deserialize[T](serializedOp)
    value.objectInspectors = KryoSerializer.deserialize(serializedObjectInspectors)
    val timeTaken = System.currentTimeMillis() - start
    logDebug("Deserializing %s took %d ms (%d bytes op, %d bytes object inspectors)".format(
      value.getClass.getName, timeTaken, opSize, objectInspectorsSize))
  }

  override def writeExternal(out: ObjectOutput) {
    val start: Long = System.currentTimeMillis()
    val serializedOp: Array[Byte] = XmlSerializer.serialize(value)
    val serializedObjectInspectors: Array[Byte] = KryoSerializer.serialize(value.objectInspectors)
    out.writeInt(serializedOp.length)
    out.writeInt(serializedObjectInspectors.length)
    out.write(serializedOp)
    out.write(serializedObjectInspectors)
    val timeTaken = System.currentTimeMillis() - start
    logDebug("Serializing %s took %d ms (%d bytes op, %d bytes object inspectors)".format(
      value.getClass.getName, timeTaken, serializedOp.length, serializedObjectInspectors.length))
  }

  override def toString(): String = {
    if (value != null) {
      "OperatorSerializationWrapper[ " + value.toString() + " ]"
    } else {
      super.toString()
    }
  }
}


object OperatorSerializationWrapper {
  def apply[T <: Operator[_ <: HiveOperator]](value: T): OperatorSerializationWrapper[T] = {
    val wrapper = new OperatorSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}


/**
 * Java object serialization using XML encoder/decoder. Avoid using this to
 * serialize byte arrays because it is extremely inefficient.
 */
object XmlSerializer {

  def serialize[T](o: T): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val e = new XMLEncoder(out)
    // workaround for java 1.5
    e.setPersistenceDelegate(classOf[ExpressionTypes], new EnumDelegate())
    e.setPersistenceDelegate(classOf[GroupByDesc.Mode], new EnumDelegate())
    e.writeObject(o)
    e.close()
    out.toByteArray()    
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    val d: XMLDecoder = new XMLDecoder(new ByteArrayInputStream(bytes))
    val ret = d.readObject()
    d.close()
    ret.asInstanceOf[T]
  }

}


/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
object KryoSerializer extends shark.LogHelper {

  @transient val kryoSer = new spark.KryoSerializer

  def serialize[T](o: T): Array[Byte] = {
    kryoSer.newInstance().serialize(o)
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    kryoSer.newInstance().deserialize[T](bytes)
  }

}
