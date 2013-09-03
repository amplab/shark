package shark.execution.cg

import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import java.util.Map
import scala.collection.mutable.{HashMap,SynchronizedMap}

object CGBeanUtils {
  def instance[T](clz: String, args: Array[Object]): T = 
    Thread.currentThread().getContextClassLoader().loadClass(clz).getDeclaredConstructors()(0).newInstance(args: _*).asInstanceOf[T]
  
  def instance[T](clz: String): T = Thread.currentThread().getContextClassLoader().loadClass(clz).newInstance().asInstanceOf[T]
}
