package shark.execution

import java.util.{HashMap => JavaHashMap, List => JList}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{JoinOperator => HiveJoinOperator}
import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, Serializer, SerDeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.io.BytesWritable

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import spark.RDD
import spark.rdd.UnionRDD
import spark.SparkContext._


class JoinOperator extends CommonJoinOperator[JoinDesc, HiveJoinOperator]
  with HiveTopOperator {

  @BeanProperty var valueTableDescMap: JavaHashMap[Int, TableDesc] = _
  @BeanProperty var keyTableDesc: TableDesc = _

  @transient var tagToValueSer: JavaHashMap[Int, Deserializer] = _
  @transient var keyDeserializer: Deserializer = _
  @transient var keyObjectInspector: StandardStructObjectInspector = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    valueTableDescMap = new JavaHashMap[Int, TableDesc]
    valueTableDescMap ++= keyValueTableDescs.map { case(tag, kvdescs) => (tag, kvdescs._2) }
    keyTableDesc = keyValueTableDescs.head._2._1

    // Call initializeOnSlave to initialize the join filters, etc.
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    tagToValueSer = new JavaHashMap[Int, Deserializer]
    valueTableDescMap foreach { case(tag, tableDesc) =>
      logDebug("tableDescs (tag %d): %s".format(tag, tableDesc))

      val deserializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Deserializer]
      deserializer.initialize(null, tableDesc.getProperties())

      logDebug("value deser (tag %d): %s".format(tag, deserializer))
      tagToValueSer.put(tag, deserializer)
    }

    if (nullCheck) {
      keyDeserializer = keyTableDesc.getDeserializerClass.newInstance.asInstanceOf[Deserializer]
      keyDeserializer.initialize(null, keyTableDesc.getProperties())
      keyObjectInspector =
        keyDeserializer.getObjectInspector().asInstanceOf[StandardStructObjectInspector]
    }
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    // Determine the number of reduce tasks to run.
    var numReduceTasks = hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS)
    if (numReduceTasks < 1) {
      numReduceTasks = 1
    }

    // Turn the RDD into a map. Use a Java HashMap to avoid Scala's annoying
    // Some/Option. Add an assert for sanity check. If ReduceSink's join tags
    // are wrong, the hash entries might collide.
    val rddsJavaMap = new JavaHashMap[Int, RDD[_]]
    rddsJavaMap ++= rdds
    assert(rdds.size == rddsJavaMap.size, {
      logError("rdds.size (%d) != rddsJavaMap.size (%d)".format(rdds.size, rddsJavaMap.size))
    })

    val rddsInJoinOrder = order.map { inputIndex =>
      rddsJavaMap.get(inputIndex.byteValue.toInt).asInstanceOf[RDD[(ReduceKey, Any)]]
    }

    val labeledRdds = rddsInJoinOrder.zipWithIndex.map { case(rdd, index) =>
      rdd.map { case(k, v) => (k, (index.toByte, new BytesWritable(v.asInstanceOf[Array[Byte]]))) }
    }
    val union = new UnionRDD(rddsInJoinOrder.head.context, labeledRdds)

    val op = OperatorSerializationWrapper(this)
    union.groupByKey(numReduceTasks).mapPartitions { part =>
      op.initializeOnSlave()

      val bufs = Array.fill(op.numTables)(new ArrayBuffer[Any])
      val tmp = new Array[Object](2)
      val writable = new BytesWritable
      val nullSafes = op.conf.getNullSafes()

      val cp = new CartesianProduct[Any](op.numTables)

      part.flatMap { case (k, seq) =>

        bufs.foreach(_.clear())
        seq.foreach { case(label, value) =>
          bufs(label) += value
          /* Ignore Hive's join filters because of its weird semantics.
          if (op.joinFilters == null || op.joinFilters.size() == 0) {
            bufs(label) += value
          } else {
            // If there are join filters, we deserialize the tuples twice. Once
            // here in checking for join filters, and one more in processPartition.
            tmp(1) = op.deserialize(op.tagToValueSer.get(label.toInt), value)
            if (!CommonJoinOperator.isFiltered(
              tmp, op.joinFilters(label), op.joinFilterObjectInspectors(label))) {
              bufs(label) += value
            }
          }
          */
        }

        writable.set(k.bytes)

        if (op.nullCheck &&
            SerDeUtils.hasAnyNullObject(
              op.keyDeserializer.deserialize(writable).asInstanceOf[JList[_]],
              op.keyObjectInspector,
              nullSafes)) {
          bufs.zipWithIndex.flatMap { case (buf, label) =>
            val bufsNull = Array.fill(op.numTables)(ArrayBuffer[Any]())
            bufsNull(label) = buf
            op.generateTuples(
              cp.product(bufsNull.asInstanceOf[Array[Seq[Any]]], op.joinConditions))
          }
        } else {
          op.generateTuples(
            cp.product(bufs.asInstanceOf[Array[Seq[Any]]], op.joinConditions))
        }
      }
    }
  }

  def generateTuples(iter: Iterator[Array[Any]]): Iterator[_] = {

    val tupleOrder = CommonJoinOperator.computeTupleOrder(joinConditions)

    //val bytes = new BytesWritable()
    val tmp = new Array[Object](2)

    val tupleSizes = (0 until joinVals.size).map { i => joinVals.get(i.toByte).size() }.toIndexedSeq
    val offsets = tupleSizes.scanLeft(0)(_ + _)

    val rowSize = offsets.last
    val outputRow = new Array[Object](rowSize)

    iter.map { elements: Array[Any] =>
      var index = 0
      while (index < numTables) {
        val element = elements(index)
        var i = 0
        if (element == null) {
          while (i < joinVals.get(index.toByte).size) {
            outputRow(i + offsets(index)) = null
            i += 1
          }
        } else {
          tmp(1) = tagToValueSer.get(index).deserialize(element.asInstanceOf[BytesWritable])
          val joinVal = joinVals.get(index.toByte)
          while (i < joinVal.size) {
            outputRow(i + offsets(index)) = joinVal(i).evaluate(tmp)
            i += 1
          }
        }
        index += 1
      }

      outputRow
    }
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] =
    throw new UnsupportedOperationException("JoinOperator.processPartition()")
}
