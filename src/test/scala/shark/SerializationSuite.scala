package shark

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer
import shark.exec.{KryoSerializationWrapper, kryoWrapper2object}
import spark.JavaSerializer

class SerializationSuite extends FunSuite {

  test("Java serializing ObjectInspectorsWrapper") {
    
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val ois = KryoSerializationWrapper(new ArrayBuffer[ObjectInspector])
    ois.value += oi
    
    val ser = new JavaSerializer
    val bytes = ser.newInstance().serialize(ois)
    val desered = ser.newInstance()
      .deserialize[KryoSerializationWrapper[ArrayBuffer[ObjectInspector]]](bytes)

    assert(desered.head.getTypeName() === oi.getTypeName())
  }
}


