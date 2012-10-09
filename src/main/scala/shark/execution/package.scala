package shark


package object execution {

  type HiveOperator = org.apache.hadoop.hive.ql.exec.Operator[_]

  implicit def opSerWrapper2op[T <: Operator[_ <: HiveOperator]](
      wrapper: OperatorSerializationWrapper[T]): T = wrapper.value

  implicit def kryoWrapper2object[T](wrapper: KryoSerializationWrapper[T]): T = wrapper.value
}

