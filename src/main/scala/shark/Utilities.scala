package shark

import java.beans.XMLEncoder
import java.beans.XMLDecoder
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.Serializable

import org.apache.hadoop.hive.ql.exec.Utilities.EnumDelegate
import org.apache.hadoop.hive.ql.plan.GroupByDesc
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes


object SharkUtilities {

  def xmlSerialize(o: Any): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val e = new XMLEncoder(out)
    // workaround for java 1.5
    e.setPersistenceDelegate(classOf[ExpressionTypes], new EnumDelegate())
    e.setPersistenceDelegate(classOf[GroupByDesc.Mode], new EnumDelegate())
    e.writeObject(o)
    e.close()
    out.toByteArray()
  }

  def xmlDeserialize(bytes: Array[Byte]): Any  = {
    val d: XMLDecoder = new XMLDecoder(new ByteArrayInputStream(bytes))
    val ret = d.readObject()
    d.close()
    ret
  }
}
