package shark.execution

import java.util.{HashMap => JavaHashMap, List => JavaList}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{JoinOperator => HiveJoinOperator}
import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
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

  @transient var tagToValueSer: JavaHashMap[Int, Deserializer] = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    valueTableDescMap = new JavaHashMap[Int, TableDesc]
    valueTableDescMap ++= keyValueTableDescs.map { case(tag, kvdescs) => (tag, kvdescs._2) }

    // Call initializeOnSlave to initialize the join filters, etc.
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    tagToValueSer = new JavaHashMap[Int, Deserializer]
    valueTableDescMap foreach { case(tag, tableDescs) =>
      logDebug("tableDescs (tag %d): %s".format(tag, tableDescs))
      val ser = JoinOperator.createDeserializerFromTableDesc(tableDescs)
      logDebug("value deser (tag %d): %s".format(tag, ser))
      tagToValueSer.put(tag, ser)
    }
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

      val bufs = new Array[ArrayBuffer[Any]](op.numTables)
      for (i <- 0 until op.numTables) bufs(i) = new ArrayBuffer[Any]
      val tmp = new Array[Object](2)

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
        CommonJoinOperator.cartesianProduct(bufs.asInstanceOf[Array[Seq[Any]]], op.joinConditions)
      }
    }
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {

    val tupleOrder = CommonJoinOperator.computeTupleOrder(joinConditions)

    //val bytes = new BytesWritable()
    val tmp = new Array[Object](2)

    val tupleSizes = (0 until joinVals.size).map { i => joinVals(i.toByte).size() }.toIndexedSeq
    val offsets = tupleSizes.scanLeft(0)(_ + _)

    val rowSize = offsets.last
    val row = new Array[Object](rowSize)

    // TODO: zipWithIndex is inefficient in a loop ...
    iter.map { elements =>

      elements.asInstanceOf[Seq[java.lang.Object]].zipWithIndex.foreach { case(element, index) =>
        if (element == null) {
          var i = 0
          while (i < joinVals(index.toByte).size) {
            row(i + offsets(index)) = null
            i += 1
          }
        } else {
          tmp(1) = deserialize(tagToValueSer.get(index), element.asInstanceOf[BytesWritable])
          joinVals(index.toByte).zipWithIndex.foreach { case(eval, i) =>
            row(i + offsets(index)) = eval.evaluate(tmp)
          }
        }
      }
      //println("row: " + row.toSeq)
      row
    }
  }

  def deserialize(serDe: Deserializer, bytes: BytesWritable) = {
    serDe.deserialize(bytes)
  }
}


object JoinOperator {

  // Different join types.
  val INNER_JOIN = JoinDesc.INNER_JOIN
  val LEFT_OUTER_JOIN = JoinDesc.LEFT_OUTER_JOIN
  val RIGHT_OUTER_JOIN = JoinDesc.RIGHT_OUTER_JOIN
  val FULL_OUTER_JOIN = JoinDesc.FULL_OUTER_JOIN
  val UNIQUE_JOIN = JoinDesc.UNIQUE_JOIN
  val LEFT_SEMI_JOIN = JoinDesc.LEFT_SEMI_JOIN

  def createDeserializerFromTableDesc(tableDesc: TableDesc): Deserializer = {
    val deserializer = tableDesc.getDeserializerClass.
      newInstance().asInstanceOf[Deserializer]
    deserializer.initialize(null, tableDesc.getProperties())
    deserializer
  }

/*
  /**
   * Given a list of lists, produce the cartesian products. This is an optimized
   * version that maps an index to a mixed-radix number, whose digits represent
   * the index of elements in each relation.
   */
  def cartesianProductMixedRadix[K: ClassManifest](key: K, bufs: IndexedSeq[IndexedSeq[Any]])
  : Seq[(K, IndexedSeq[Any])] = {
    val tupleSize = bufs.size
    val radixList = new Array[Int](tupleSize)
    radixList(0) = 1
    for (pos <- 1 until tupleSize) {
      radixList(pos) = radixList(pos - 1) * bufs(pos - 1).size
    }
    val numTuples = radixList.last * bufs.last.size

    val permutations = new Array[(K, IndexedSeq[Any])](numTuples)
    for (i <- 0 until numTuples) {
      val newTuple = new Array[Any](tupleSize)
      for (pos <- 0 until tupleSize) {
        newTuple(pos) = bufs(pos)((i / radixList(pos)) % bufs(pos).size)
      }
      permutations(i) = (key, newTuple.toIndexedSeq)
    }
    permutations.toSeq
  }
*/

}

