package shark

package object exec {

  type HiveOperator = org.apache.hadoop.hive.ql.exec.Operator[_]

  type MapJoinHashTable = collection.immutable.Map[java.lang.Object,Seq[Array[java.lang.Object]]]

  implicit def opSerWrapper2op[T <: Operator[_ <: HiveOperator]](
      wrapper: OperatorSerializationWrapper[T]): T = wrapper.value

  implicit def kryoWrapper2object[T](wrapper: KryoSerializationWrapper[T]): T = wrapper.value
}

