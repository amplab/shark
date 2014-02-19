package shark.execution.cg.row

import org.apache.commons.io.output.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.Serializer


/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
object KryoSerializer {

  val kryo = new Kryo()
  val output = new Output(new ByteArrayOutputStream())
  val input = new Input()
  
  def deserialize[T](bytes: Array[Byte]): T = deserialize(bytes, Thread.currentThread().getContextClassLoader())

  def deserialize[T](bytes: Array[Byte], classloader: ClassLoader) = {
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(classloader)
      input.setBuffer(bytes)
      val obj = kryo.readClassAndObject(input).asInstanceOf[T]
      
      obj
    } finally {
      kryo.setClassLoader(oldClassLoader)
    }
  }
  
  def serialize[T](o: T): Array[Byte] = { 
    output.clear()
    kryo.writeClassAndObject(output, o)
    
    output.toBytes
  }
}

/**
 * This class is used by the genereated classes to ser/de objects (which implement KryoSerializable)
 */
class KryoSerializer(klasses: Array[Class[_]] = Array[Class[_]](), 
    var cl: ClassLoader = Thread.currentThread().getContextClassLoader()) {
  
  val kryo = new Kryo()

  val output = new Output(new ByteArrayOutputStream())
  val input = new Input()
  
  {
    // register the generated class
    klasses.foreach(cls => kryo.register(cls))
  }

  def deserialize[T](bytes: Array[Byte]): T = deserialize(bytes, cl)

  def deserialize[T](bytes: Array[Byte], classloader: ClassLoader) = {
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(classloader)
      input.setBuffer(bytes)
      val obj = kryo.readClassAndObject(input).asInstanceOf[T]
      
      obj
    } finally {
      kryo.setClassLoader(oldClassLoader)
    }
  }
  
  def serialize[T](o: T): Array[Byte] = { 
    output.clear()
    kryo.writeClassAndObject(output, o)
    
    output.toBytes
  }
}
