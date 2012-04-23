package shark.exec


/**
 * A wrapper around some unserializable objects that make them both Java
 * serializable and XMLEncoder serializable. Internally, Kryo is used for
 * serialization.
 * 
 * Use SerializationWrapper(value) to create a wrapper.
 */
class KryoSerializationWrapper[T] extends Serializable {

  @transient var value: T = _

  private var valueSerialized: Array[Byte] = _

  // The getter and setter for valueSerialized is used for XML serialization.
  def getValueSerialized(): Array[Byte] = {
    valueSerialized = Operator.kryoSer.newInstance().serialize(value)
    valueSerialized
  }

  def setValueSerialized(bytes: Array[Byte]) = {
    valueSerialized = bytes
    value = Operator.kryoSer.newInstance().deserialize[T](valueSerialized)
  }

  // Used for Java serialization.
  private def writeObject(out: java.io.ObjectOutputStream) {
    getValueSerialized()
    out.defaultWriteObject()
  }

  private def readObject(in: java.io.ObjectInputStream) {
    in.defaultReadObject()
    setValueSerialized(valueSerialized)
  }
}


object KryoSerializationWrapper {  
  def apply[T](value: T): KryoSerializationWrapper[T] = {
    val wrapper = new KryoSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}

