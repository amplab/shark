package shark

import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey


package object exec {

  type HiveOperator = org.apache.hadoop.hive.ql.exec.Operator[_]

  type MapJoinHashTable = collection.immutable.Map[AbstractMapJoinKey, Seq[Array[java.lang.Object]]]

  implicit def opSerWrapper2op[T <: Operator[_ <: HiveOperator]](
      wrapper: OperatorSerializationWrapper[T]): T = wrapper.value

  implicit def kryoWrapper2object[T](wrapper: KryoSerializationWrapper[T]): T = wrapper.value
}

