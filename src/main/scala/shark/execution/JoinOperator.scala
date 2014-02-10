/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.util.{HashMap => JHashMap, List => JList}

import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.io.BytesWritable

import org.apache.spark.{CoGroupedRDD, HashPartitioner}
import org.apache.spark.rdd.RDD

import shark.execution.serialization.OperatorSerializationWrapper
import shark.io.MutableBytesWritable


class JoinOperator extends CommonJoinOperator[JoinDesc] with ReduceSinkTableDesc {

  @BeanProperty var valueTableDescMap: JHashMap[Int, TableDesc] = _
  @BeanProperty var keyTableDesc: TableDesc = _

  @transient var tagToValueSer: JHashMap[Int, Deserializer] = _
  @transient var keyDeserializer: Deserializer = _
  @transient var keyObjectInspector: StandardStructObjectInspector = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    val descs = keyValueDescs()
    valueTableDescMap = new JHashMap[Int, TableDesc]
    valueTableDescMap ++= descs.map { case(tag, kvdescs) => (tag, kvdescs._2) }
    keyTableDesc = descs.head._2._1

    // Call initializeOnSlave to initialize the join filters, etc.
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    tagToValueSer = new JHashMap[Int, Deserializer]
    valueTableDescMap foreach { case(tag, tableDesc) =>
      logDebug("tableDescs (tag %d): %s".format(tag, tableDesc))

      val deserializer = tableDesc.getDeserializerClass.newInstance()
      deserializer.initialize(null, tableDesc.getProperties())

      logDebug("value deser (tag %d): %s".format(tag, deserializer))
      tagToValueSer.put(tag, deserializer)
    }

    if (nullCheck) {
      keyDeserializer = keyTableDesc.getDeserializerClass.newInstance()
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
    val rddsJavaMap = new JHashMap[Int, RDD[_]]
    rddsJavaMap ++= rdds
    assert(rdds.size == rddsJavaMap.size, {
      logError("rdds.size (%d) != rddsJavaMap.size (%d)".format(rdds.size, rddsJavaMap.size))
    })

    val rddsInJoinOrder = order.map { inputIndex =>
      rddsJavaMap.get(inputIndex.byteValue.toInt).asInstanceOf[RDD[(ReduceKey, Any)]]
    }

    val part = new HashPartitioner(numReduceTasks)
    val cogrouped = new CoGroupedRDD[ReduceKey](
      rddsInJoinOrder.toSeq.asInstanceOf[Seq[RDD[(_, _)]]], part)

    val op = OperatorSerializationWrapper(this)

    cogrouped.mapPartitions { part =>
      op.initializeOnSlave()

      val writable = new BytesWritable
      val nullSafes = op.conf.getNullSafes()

      val cp = new CartesianProduct[Any](op.numTables)

      part.flatMap { case (k: ReduceKeyReduceSide, bufs: Array[_]) =>
        writable.set(k.byteArray, 0, k.length)

        // If nullCheck is false, we can skip deserializing the key.
        if (op.nullCheck &&
            SerDeUtils.hasAnyNullObject(
              op.keyDeserializer.deserialize(writable).asInstanceOf[JList[_]],
              op.keyObjectInspector,
              nullSafes)) {
          bufs.iterator.zipWithIndex.flatMap { case (buf, label) =>
            val bufsNull = Array.fill(op.numTables)(ArrayBuffer[Any]())
            bufsNull(label) = buf
            op.generateTuples(cp.product(bufsNull.asInstanceOf[Array[Seq[Any]]], op.joinConditions))
          }
        } else {
          op.generateTuples(cp.product(bufs.asInstanceOf[Array[Seq[Any]]], op.joinConditions))
        }
      }
    }
  }

  def generateTuples(iter: Iterator[Array[Any]]): Iterator[_] = {
    new TupleIterator(iter)
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("JoinOperator.processPartition()")

  class TupleIterator(iter: Iterator[Array[Any]]) extends Iterator[Object] {
    val tupleSizes = joinVals.map((e) => e.size).toIndexedSeq
    val offsets = tupleSizes.scanLeft(0)(_ + _)

    val rowSize = offsets.last

    val outputs = Queue[Array[Object]]()
    var done = false
    
    def hasNext = {
      if(outputs.isEmpty) {
        processNext()
      }
      
      !outputs.isEmpty
    }
    
    def next = {
      if(outputs.isEmpty) {
        processNext()
      }

      outputs.dequeue
    }
    
    private def processNext() {
      if(!done) {
        // if not set as finished, iterate the next
        var continue = (outputs.isEmpty) // if not element in the queue, the try to get
        
        while(continue) {
          if (iter.hasNext) {
            var elements: Array[Any] = iter.next

            val row = new Array[Object](rowSize)
            done = generate(false, null, 0, elements, row, outputs)
            
            if(done || outputs.size() > 0) {
              // if no more records needed, or got record(s)
              continue = false
            }
          } else {
            continue = false
            done = true
          }
        }
      }
    }

    /**
     * Deserialize the value,.
     */
    private def deserColumns(element: Array[Byte], deser: Deserializer): Array[java.lang.Object] = {
      // TODO should reuse the kv object
      var kv = new Array[Object](2)
      if (element != null) {
        //reuseByte.set(element, 0, element.length)
        var bytes = new BytesWritable(element)
        // TODO may cause performance issue, cause the DUPLICATED table rows (in bytes) are fed 
        // in the join entries, need to use the "lazy parse object" instead of the "bytes".
        kv(1) = deser.deserialize(bytes)
      
        kv
      } else {
        null
      }
    }

    /**
     * Create the new output tuple(s), and put the output result into the outputRows.
     * NOTICE: Single input entry("elements") can creates 0 or 1 or 2 output rows due to the 
     * join filter.
     */
    private def generate(previousRightFiltered: Boolean, previousRightTable: Array[Object], 
      startIdx: Int, elements: Array[Any], row: Array[Object],
      outputRows: Queue[Array[Object]]): Boolean = {
      
      var index = startIdx
      var done = false

      var leftTable = if(index == 0) {
        deserColumns(elements(index).asInstanceOf[Array[Byte]], tagToValueSer.get(index))
      } else {
        previousRightTable
      }
      
      var leftFiltered = if(index == 0) {
        // check the join filter (true for discard the data)
        CommonJoinOperator.isFiltered(leftTable,
          joinFilters(index), joinFilterObjectInspectors(index.toByte))
      } else {
        previousRightFiltered
      }

      var entireLeftFiltered = leftFiltered
      
      while (index < joinConditions.size) {
        var joinCondition = joinConditions(index)
        var rightTableIndex  = index + 1
        
        var rightTable = deserColumns(elements(rightTableIndex).asInstanceOf[Array[Byte]], 
            tagToValueSer.get(rightTableIndex))
        var rightFiltered = CommonJoinOperator.isFiltered(rightTable,
          joinFilters(rightTableIndex.toByte), 
          joinFilterObjectInspectors(rightTableIndex.toByte))

        joinCondition.getType() match {
          case CommonJoinOperator.FULL_OUTER_JOIN => {
            /**
             * if one of the node doesn't pass the filter test(filtered=true)
             * will generate 2 rows:
             * 1) keep the right table columns, and reset the left tables columns
             * 2) keep the left tables columns, and reset the right table columns
             */
            if (entireLeftFiltered || rightFiltered) {
              // Row 1: keep the right table columns, and discard left tables
              // create a new row object, with null value for all of the columns
              var row2 = new Array[java.lang.Object](row.length)
              generate((rightTable == null), rightTable, 
                  rightTableIndex, elements, row2, outputRows)
              
              // Row 2: keep the left table columns, and discard the right table
              rightFiltered = true
            } else {
              rightFiltered = false
            }
            leftFiltered = false
          }
          case CommonJoinOperator.LEFT_OUTER_JOIN => {
            if (entireLeftFiltered || rightFiltered) {
              // will not output anything for the right table columns
              rightFiltered = true
            }
            leftFiltered = false
          }
          case CommonJoinOperator.RIGHT_OUTER_JOIN => {
            // setColumnValues(rightTable, row, rightTableIndex.toByte, offsets)
            
            if (entireLeftFiltered || rightFiltered) {
              // if filtered then reset all of the left tables columns
              java.util.Arrays.fill(row, 0, offsets(rightTableIndex.toByte), null)
              leftFiltered = true
            }
            rightFiltered = false
          }
          case CommonJoinOperator.LEFT_SEMI_JOIN => {
            // the same with left outer join, but only output the first valid row
            if (entireLeftFiltered || rightFiltered) {
              // will not output anything for the right table columns
              rightFiltered = true
            } else {
              // find the valid output, and will not output new row any more
              done = true
            }
            leftFiltered = false
          }
          case CommonJoinOperator.INNER_JOIN => {
            if (entireLeftFiltered || rightFiltered) {
              java.util.Arrays.fill(row, 0, offsets(joinCondition.getRight().toByte), null)
              leftFiltered  = true
              rightFiltered = true
            }
          }
          case _ => assert(false)
        }
        
        leftFiltered = leftFiltered || (leftTable == null)
        rightFiltered = rightFiltered || (rightTable == null)
        
        // set the left table columns
        if(!leftFiltered) {
          setColumnValues(leftTable, row, index.toByte, offsets)
        }

        entireLeftFiltered = entireLeftFiltered && leftFiltered && rightFiltered
        
        leftFiltered = rightFiltered
        leftTable = rightTable
        index += 1
      }
      
      // set the most right table columns
      if(!leftFiltered) {
        setColumnValues(leftTable, row, index.toByte, offsets)
      }
      
      entireLeftFiltered = entireLeftFiltered && leftFiltered
      
      if(entireLeftFiltered)
        done = false
      else
        outputRows.enqueue(row)

      done
    }

    /**
     * Set the columns for the join result of the specified table
     */
    private def setColumnValues(data: Object, outputRow: Array[Object], tblIdx: Byte,
                                offsets: IndexedSeq[Int]) {

      val joinVal = joinVals(tblIdx)
      val joinValOIs = joinValuesObjectInspectors(tblIdx)
      
      var idx = 0
      val size = joinVal.size()
      
      while (idx < size) {
        outputRow(idx + offsets(tblIdx.toInt)) = ObjectInspectorUtils.copyToStandardObject(
          joinVal(idx).evaluate(data), joinValOIs(idx),
          ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE)
        
        idx += 1
      }
    }
  }
}


