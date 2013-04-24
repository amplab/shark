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

import java.util.Arrays

import scala.collection.mutable.StringBuilder

import org.apache.hadoop.io.{BytesWritable, WritableComparator}

import spark.HashPartitioner


/**
 * A data structure used for shuffling data that supports comparison. We wrap
 * ReduceKey around a normal key byte array so the byte array can be used in
 * binary comparison, as well as enabling partitioning data based on the
 * partitionCode field using a ReduceKeyPartitioner.
 */
class ReduceKey(var bytes: BytesWritable = null) extends Ordered[ReduceKey] {

  /** Used by ReduceKeyPartitioner to determine the hash partition. */
  @transient var partitionCode = 0

  def getLength(): Int = bytes.getLength()

  def getBytes(): Array[Byte] = bytes.getBytes()

  override def equals(other: Any): Boolean  = {
    other match {
      case other: ReduceKey => bytes.equals(other.bytes)
      case _ => false
    }
  }

  override def compare(that: ReduceKey): Int = this.bytes.compareTo(that.bytes)

  override def hashCode() = bytes.hashCode()

  override def toString() = bytes.toString()
}


/**
 * A special Spark partitioner that allows hash partitioning of data based on
 * the partitionCode field in ReduceKey.
 */
class ReduceKeyPartitioner(partitions: Int) extends HashPartitioner(partitions) {

  override def getPartition(key: Any): Int = {
    key match {
      case k: ReduceKey => {
        val mod = k.partitionCode % partitions
        if (mod < 0) mod + partitions else mod  // Guard against negative hash codes.
      }
      case other => {
        throw new Exception(
          "ReduceKeyPartitioner expects object of class ReduceKey, but got " +
          other.getClass.getName)
      }
    }
  }

}
