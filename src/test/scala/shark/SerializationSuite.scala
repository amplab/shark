package shark

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer
import shark.exec.{KryoSerializationWrapper, kryoWrapper2object}
import spark.JavaSerializer

class SerializationSuite extends FunSuite {

  test("Java serializing object inspectors") {
    
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val ois = KryoSerializationWrapper(new ArrayBuffer[ObjectInspector])
    ois.value += oi
    
    val ser = new JavaSerializer
    val bytes = ser.newInstance().serialize(ois)
    val desered = ser.newInstance()
      .deserialize[KryoSerializationWrapper[ArrayBuffer[ObjectInspector]]](bytes)

    assert(desered.head.getTypeName() === oi.getTypeName())
  }

  test("Java serializing operators") {
    val operator = new shark.exec.FileSinkOperator
    operator.localHconf = new org.apache.hadoop.hive.conf.HiveConf
    operator.localHiveOp = new org.apache.hadoop.hive.ql.exec.FileSinkOperator
    val opWrapped = shark.exec.OperatorSerializationWrapper(operator)

    val ser = new JavaSerializer
    val bytes = ser.newInstance().serialize(opWrapped)
    val desered = ser.newInstance()
      .deserialize[shark.exec.OperatorSerializationWrapper[shark.exec.FileSinkOperator]](bytes)

    assert(desered.value != null)
    assert(desered.value.localHconf != null)
    assert(desered.value.localHiveOp != null)
  }
}


