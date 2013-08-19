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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import org.apache.hadoop.io.{BytesWritable, Writable}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.binarysortable.{BinarySortableSerDe, OutputByteBuffer, 
  HiveStructPartialSerializer}

import shark.{CoPartitioner, SharkConfVars, SharkEnv, SharkEnvSlave, Utils}
import shark.execution.serialization.{OperatorSerializationWrapper, JavaSerializer}
import shark.memstore2._
import shark.tachyon.TachyonTableWriter

import spark.{RDD, SparkEnv, TaskContext}
import spark.rdd.ShuffledWithLocationsRDD
import spark.SparkContext._
import spark.storage.StorageLevel


/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class MemoryStoreSinkOperator extends TerminalOperator {

  @BeanProperty var initialColumnSize: Int = _
  @BeanProperty var storageLevel: StorageLevel = _
  @BeanProperty var tableName: String = _
  @transient var useTachyon: Boolean = _
  @transient var useUnionRDD: Boolean = _
  @transient var numColumns: Int = _
  @BeanProperty var coPartitionTableName: String = _
  @BeanProperty var partColInternalName: Array[String] = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    initialColumnSize = SharkConfVars.getIntVar(localHconf, SharkConfVars.COLUMN_INITIALSIZE)
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    localHconf.setInt(SharkConfVars.COLUMN_INITIALSIZE.varname, initialColumnSize)
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val op = OperatorSerializationWrapper(this)
    val shouldPartition = partColInternalName.size != 0
    val partCols = partColInternalName
 
    def getCacheLocations(rdd: RDD[_]): Array[Seq[String]] = {
      val blockIds = rdd.partitions.indices.map(index=> "rdd_%d_%d".format(rdd.id, index)).toArray
      SparkEnv.get.blockManager.master.getLocations(blockIds).map {
        locations => locations.map(_.hostPort).toSeq
      }.toArray
    } 

    val tachyonWriter: TachyonTableWriter =
      if (useTachyon) {
        // Use an additional row to store metadata (e.g. number of rows in each partition).
        SharkEnv.tachyonUtil.createTableWriter(tableName, numColumns + 1)
      } else {
        null
      }

    val rddToCache = {
      if (shouldPartition) {
        val (partitioner, locations) = {
          val partNum = localHconf.getInt("shark.copartition.partnum", 5)
          SharkEnv.memoryMetadataManager.get(coPartitionTableName) match {
            case Some(coRdd) => logInfo("Copartitioning table %s with %s ".
                                        format(tableName, coPartitionTableName))
              (new CoPartitioner(partNum), getCacheLocations(coRdd))
            case _ => logInfo("Copartition table not found in cache:" + coPartitionTableName)
              (new CoPartitioner(partNum), null)
          }
        }
        partCols.foreach(partitioner.addPartitionCol(_))
        val kvRdd = inputRdd.mapPartitions { rowIter => 
          op.initializeOnSlave()
          val serde = new BinarySortableSerDe()
          serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties)
          val sois = op.objectInspector.asInstanceOf[StructObjectInspector]
          val fields = partCols.map(sois.getStructFieldRef(_))
          val bserializer = new HiveStructPartialSerializer(sois, fields)
          rowIter.map { row =>
            val key = bserializer.serialize(row.asInstanceOf[AnyRef]).asInstanceOf[BytesWritable]
            val value = serde.serialize(row, op.objectInspector).asInstanceOf[BytesWritable]
            val reduceKey = new ReduceKeyMapSide(key)
            (reduceKey, value)
          }
        }
        logInfo("kvRdd generated for table with copartitionCol " + partColInternalName + " " + partitioner)
        new ShuffledWithLocationsRDD[Any, Any](kvRdd.asInstanceOf[RDD[(Any, Any)]], 
            partitioner, locations).mapPartitions(iter => {
              iter.map { case (k: ReduceKeyReduceSide, v: Array[Byte]) => v }
            }, true)
      } else {
        inputRdd
      }
    }

    // Put all rows of the table into a set of TablePartition's. Each partition contains
    // only one TablePartition object.
    var rdd: RDD[TablePartition] = rddToCache.mapPartitionsWithIndex({ case(partitionIndex, iter) =>
      op.initializeOnSlave()
      val serde = new ColumnarSerDe
      serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties())

      val (iterToUse, ois) = if (shouldPartition) {
        val serdeDeserialize = new BinarySortableSerDe()
        serdeDeserialize.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties)
        val bw = new BytesWritable()
        val mappedIter = iter.map { x => 
          bw.set(x.asInstanceOf[Array[Byte]], 0, x.asInstanceOf[Array[Byte]].length)
          serdeDeserialize.deserialize(bw)
        }
        (mappedIter, serdeDeserialize.getObjectInspector)
      } else
        (iter, op.objectInspector)

      // Serialize each row into the builder object.
      // ColumnarSerDe will return a TablePartitionBuilder.
      var builder: Writable = null
      iterToUse.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], ois)
      }

      if (builder != null) {
        statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      } else {
        // Empty partition.
        statsAcc += Tuple2(partitionIndex, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      }
    }, preservesPartitioning = true)

    if (tachyonWriter != null) {
      // Put the table in Tachyon.
      op.logInfo("Putting RDD for %s in Tachyon".format(tableName))

      SharkEnv.memoryMetadataManager.put(tableName, rdd)

      tachyonWriter.createTable(ByteBuffer.allocate(0))
      rdd = rdd.mapPartitionsWithIndex { case(partitionIndex, iter) =>
        val partition = iter.next()
        partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
          tachyonWriter.writeColumnPartition(column, partitionIndex, buf)
        }
        Iterator(partition)
      }
      // Force evaluate so the data gets put into Tachyon.
      rdd.foreach(_ => Unit)
    } else {
      // Put the table in Spark block manager.
      op.logInfo("Putting %sRDD for %s in Spark block manager, %s %s %s %s".format(
        if (useUnionRDD) "Union" else "",
        tableName,
        if (storageLevel.deserialized) "deserialized" else "serialized",
        if (storageLevel.useMemory) "in memory" else "",
        if (storageLevel.useMemory && storageLevel.useDisk) "and" else "",
        if (storageLevel.useDisk) "on disk" else ""))

      // Force evaluate so the data gets put into Spark block manager.
      rdd.persist(storageLevel)
      rdd.foreach(_ => Unit)

      if (useUnionRDD) {
        rdd = rdd.union(
          SharkEnv.memoryMetadataManager.get(tableName).get.asInstanceOf[RDD[TablePartition]])
      }
      SharkEnv.memoryMetadataManager.put(tableName, rdd)
    }

    // Report remaining memory.
    /* Commented out for now waiting for the reporting code to make into Spark.
    val remainingMems: Map[String, (Long, Long)] = SharkEnv.sc.getSlavesMemoryStatus
    remainingMems.foreach { case(slave, mem) =>
      println("%s: %s / %s".format(
        slave,
        Utils.memoryBytesToString(mem._2),
        Utils.memoryBytesToString(mem._1)))
    }
    println("Summary: %s / %s".format(
      Utils.memoryBytesToString(remainingMems.map(_._2._2).sum),
      Utils.memoryBytesToString(remainingMems.map(_._2._1).sum)))
    */

    var columnStats =
      if (useUnionRDD) {
        // Combine stats for the two tables being combined.
        val numPartitions = statsAcc.value.toMap.size
        val currentStats = statsAcc.value
        val otherIndexToStats = SharkEnv.memoryMetadataManager.getStats(tableName).get
        for ((otherIndex, tableStats) <- otherIndexToStats) {
          currentStats.append((otherIndex + numPartitions, tableStats))
        }
        currentStats.toMap
      } else {
        statsAcc.value.toMap
      }

    // Get the column statistics back to the cache manager.
    SharkEnv.memoryMetadataManager.putStats(tableName, columnStats)

    if (tachyonWriter != null) {
      tachyonWriter.updateMetadata(ByteBuffer.wrap(JavaSerializer.serialize(columnStats)))
    }

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      columnStats.foreach { case(index, tableStats) =>
        println("Partition " + index + " " + tableStats.toString)
      }
    }

    // Return the cached RDD.
    rdd
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}
