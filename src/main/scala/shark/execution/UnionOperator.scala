package shark.execution

import java.util.{ArrayList, List => JavaList}

import org.apache.hadoop.hive.ql.exec.{UnionOperator => HiveUnionOperator}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.SharkEnv
import spark.{UnionRDD, RDD, Split}


/**
 * A union operator. If the incoming data are of different type, the union
 * operator transforms the incoming data into the same type.
 */
class UnionOperator extends NaryOperator[HiveUnionOperator] {

  @transient var parentFields: ArrayBuffer[JavaList[_ <: StructField]] = _
  @transient var parentObjInspectors: ArrayBuffer[StructObjectInspector] = _
  @transient var columnTypeResolvers: Array[ReturnObjectInspectorResolver] = _

  @BeanProperty var needsTransform: Array[Boolean] = _
  @BeanProperty var numParents: Int = _

  override def initializeOnMaster() {
    numParents = parentOperators.size

    // Use reflection to get the needsTransform boolean array.
    val needsTransformField = hiveOp.getClass.getDeclaredField("needsTransform")
    needsTransformField.setAccessible(true)
    needsTransform = needsTransformField.get(hiveOp).asInstanceOf[Array[Boolean]]

    initializeOnSlave()
  }

  override def initializeOnSlave() {
    // Some how in union, it is possible for Hive to add an extra null object
    // inspectors. We need to work around that.
    parentObjInspectors = objectInspectors.filter(_ != null)
        .map(_.asInstanceOf[StructObjectInspector])
    parentFields = parentObjInspectors.map(_.getAllStructFieldRefs())

    // Get columnNames from the first parent
    val columns = parentFields.head.size()
    val columnNames = parentFields.head.map(_.getFieldName())

    // Get outputFieldOIs
    columnTypeResolvers = new Array[ReturnObjectInspectorResolver](columns)
    for (c <- 0 until columns) columnTypeResolvers(c) = new ReturnObjectInspectorResolver()

    for (p <- 0 until numParents) {
      assert(parentFields(p).size() == columns)
      for (c <- 0 until columns) {
        columnTypeResolvers(c).update(parentFields(p).get(c).getFieldObjectInspector())
      }
    }

    val outputFieldOIs = columnTypeResolvers.map(_.get())
    val outputObjInspector = SharkEnv.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, outputFieldOIs.toList)
    }

    // whether we need to do transformation for each parent
    // We reuse needsTransform from Hive because the comparison of object
    // inspectors are hard once we send object inspectors over the wire.
    needsTransform.zipWithIndex.filter(_._1).foreach { case(transform, p) =>
      logInfo("Union Operator needs to transform row from parent[%d] from %s to %s".format(
          p, objectInspectors(p), outputObjInspector))
    }
  }

  /**
   * Override execute. The only thing we need to call is combineMultipleRdds().
   */
  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    val rddsInOrder: Seq[RDD[Any]] = rdds.sortBy(_._1).map(_._2.asInstanceOf[RDD[Any]])

    val rddsTransformed = rddsInOrder.zipWithIndex.map { case(rdd, tag) =>
      if (needsTransform(tag)) {
        transformRdd(rdd, tag)
      } else {
        rdd
      }
    }

    new UnionRDD(rddsTransformed.head.context, rddsTransformed.asInstanceOf[Seq[RDD[Any]]])
  }

  def transformRdd(rdd: RDD[_], tag: Int) = {
    // Since Union does not rely on the general Operator structure, we need
    // to serialize the object inspectors ourselves.
    val op = OperatorSerializationWrapper(this)

    rdd.mapPartitions { part =>
      op.initializeOnSlave()

      val columns = op.parentFields.head.size()
      val outputRow = new ArrayList[Object](columns)
      for (c <- 0 until columns) outputRow.add(null)

      part.map { row =>
        val soi = op.parentObjInspectors(tag)
        val fields = op.parentFields(tag)

        for (c <- 0 until fields.size) {
          outputRow.set(c, op.columnTypeResolvers(c).convertIfNecessary(soi
              .getStructFieldData(row, fields.get(c)), fields.get(c)
              .getFieldObjectInspector()))
        }

        outputRow
      }
    }
  }

  override def processPartition(split: Split, iter: Iterator[_]): Iterator[_] = {
    throw new Exception("UnionOperator.processPartition() should've never been called.")
  }
}

