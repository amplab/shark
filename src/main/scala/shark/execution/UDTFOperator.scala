package shark.execution

import java.util.{List => JavaList}

import org.apache.hadoop.hive.ql.exec.{UDTFOperator => HiveUDTFOperator}
import org.apache.hadoop.hive.ql.plan.UDTFDesc
import org.apache.hadoop.hive.ql.udf.generic.Collector
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector,
  StandardStructObjectInspector, StructField, StructObjectInspector }

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import spark.Split


class UDTFOperator extends UnaryOperator[HiveUDTFOperator] {

  @BeanProperty var conf: UDTFDesc = _

  @transient var objToSendToUDTF: Array[java.lang.Object] = _
  @transient var soi: StandardStructObjectInspector = _
  @transient var inputFields: JavaList[_ <: StructField] = _
  @transient var collector: UDTFCollector = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }

  override def initializeOnSlave() {
    collector = new UDTFCollector
    conf.getGenericUDTF().setCollector(collector)

    // Make an object inspector [] of the arguments to the UDTF
    soi = objectInspectors.head.asInstanceOf[StandardStructObjectInspector]
    inputFields = soi.getAllStructFieldRefs()

    val udtfInputOIs = inputFields.map { case inputField =>
      inputField.getFieldObjectInspector()
    }.toArray

    objToSendToUDTF = new Array[java.lang.Object](inputFields.size)
    val udtfOutputOI = conf.getGenericUDTF().initialize(udtfInputOIs)
  }

  override def processPartition(split: Split, iter: Iterator[_]): Iterator[_] = {
    iter.flatMap { row =>
      explode(row)
    }
  }

  def explode[T](row: T): ArrayBuffer[java.lang.Object] = {
    (0 until inputFields.size).foreach { case i =>
      objToSendToUDTF(i) = soi.getStructFieldData(row, inputFields.get(i))
    }
    conf.getGenericUDTF().process(objToSendToUDTF)
    collector.collectRows()
  }
}

class UDTFCollector extends Collector {

  var collected = new ArrayBuffer[java.lang.Object]

  override def collect(input: java.lang.Object) {
    // We need to clone the input here because implementations of
    // GenericUDTF reuse the same object. Luckily they are always an array, so
    // it is easy to clone.
    collected += input.asInstanceOf[Array[_]].clone
  }

  def collectRows() = {
    val toCollect = collected
    collected = new ArrayBuffer[java.lang.Object]
    toCollect
  }

}
