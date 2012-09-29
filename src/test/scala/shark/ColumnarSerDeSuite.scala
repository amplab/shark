package shark

import org.apache.hadoop.hive.serde2.objectinspector.{
  PrimitiveObjectInspector,
  ObjectInspectorFactory,
  StandardListObjectInspector,
  StandardMapObjectInspector
}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  PrimitiveObjectInspectorUtils,
  PrimitiveObjectInspectorFactory
}
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import scala.collection.JavaConversions._
import shark.memstore._

class ColumnarSerDeSuite extends FunSuite {

  def size = ColumnarSerDe.getFieldSize _

  test("ColumnarSerDe.size primitive") {
    assert(size(createPrimitiveOi(classOf[java.lang.Boolean])) == 1)
    assert(size(createPrimitiveOi(classOf[java.lang.Byte])) == 1)
    assert(size(createPrimitiveOi(classOf[java.lang.Short])) == 2)
    assert(size(createPrimitiveOi(classOf[java.lang.Integer])) == 4)
    assert(size(createPrimitiveOi(classOf[java.lang.Long])) == 8)
    assert(size(createPrimitiveOi(classOf[java.lang.Float])) == 4)
    assert(size(createPrimitiveOi(classOf[java.lang.Double])) == 8)
  }

  test("ColumnarSerDe.size list") {
    val FACTOR = 5
    assert(size(createListOi(classOf[java.lang.Boolean])) == 1 * FACTOR)
    assert(size(createListOi(classOf[java.lang.Byte])) == 1 * FACTOR)
    assert(size(createListOi(classOf[java.lang.Short])) == 2 * FACTOR)
    assert(size(createListOi(classOf[java.lang.Integer])) == 4 * FACTOR)
    assert(size(createListOi(classOf[java.lang.Long])) == 8 * FACTOR)
    assert(size(createListOi(classOf[java.lang.Float])) == 4 * FACTOR)
    assert(size(createListOi(classOf[java.lang.Double])) == 8 * FACTOR)
  }

  test("ColumnarSerDe.size map") {
    val FACTOR = 5
    assert(size(createMapOi(classOf[java.lang.Boolean], classOf[java.lang.Boolean])) == 2 * FACTOR)
    assert(size(createMapOi(classOf[java.lang.Integer], classOf[java.lang.Float])) == 8 * FACTOR)
    assert(size(createMapOi(classOf[java.lang.Integer], classOf[java.lang.Double])) == 12 * FACTOR)
    assert(size(createMapOi(classOf[java.lang.Boolean], classOf[java.lang.Double])) == 9 * FACTOR)
  }

  test("ColumnarSerDe.size struct") {
    val names = List("a", "b", "c")
    val ois = List(
      createPrimitiveOi(classOf[java.lang.Boolean]),
      createPrimitiveOi(classOf[java.lang.Byte]),
      createPrimitiveOi(classOf[java.lang.Integer]))
    val structOi = ObjectInspectorFactory.getStandardStructObjectInspector(names, ois)
    assert(size(structOi) == 1 + 1 + 4)
  }

  def createPrimitiveOi(javaClass: Class[_]): PrimitiveObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
      PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaClass(javaClass).primitiveCategory)

  def createListOi(javaClass: Class[_]): StandardListObjectInspector =
    ObjectInspectorFactory.getStandardListObjectInspector(createPrimitiveOi(javaClass))

  def createMapOi(keyJavaClass: Class[_], valueJavaClass: Class[_]): StandardMapObjectInspector =
    ObjectInspectorFactory.getStandardMapObjectInspector(
      createPrimitiveOi(keyJavaClass), createPrimitiveOi(valueJavaClass))
}
