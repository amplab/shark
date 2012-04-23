package shark

import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import scala.collection.mutable.ArrayBuffer


package object exec {

  type HiveOperator = org.apache.hadoop.hive.ql.exec.Operator[_]

  type MapJoinHashTable = collection.immutable.Map[AbstractMapJoinKey, Seq[Array[java.lang.Object]]]

  implicit def serializationWrapper2Object[T](wrapper: KryoSerializationWrapper[T]): T = wrapper.value

}

