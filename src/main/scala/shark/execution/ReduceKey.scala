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

import java.io.{Externalizable, ObjectOutput, ObjectInput}

import com.google.common.primitives.UnsignedBytes

import org.apache.hadoop.io.{BytesWritable, WritableComparator}

import org.apache.spark.HashPartitioner


/**
 * A data structure used for shuffling data that supports comparison. We wrap
 * ReduceKey around a normal key byte array so the byte array can be used in
 * binary comparison, as well as enabling partitioning data based on the
 * partitionCode field using a ReduceKeyPartitioner.
 *
 * Note that this data structure needs to be serializable because in the case of a total
 * order sort, Spark's range partitioner uses a collect operation to find the ranges.
 * The collect serializes the ReduceKey objects and send them back to the master.
 */
sealed trait ReduceKey extends Comparable[ReduceKey] {

  def length: Int

  def byteArray: Array[Byte]

  override def hashCode: Int

  override def toString: String

  override def equals(other: Any): Boolean

  override def compareTo(that: ReduceKey): Int
}


class ReduceKeyMapSide(var bytesWritable: BytesWritable) extends ReduceKey
  with Externalizable {

  // A default no-arg constructor for Java serializer to use.
  def this() = this(new BytesWritable)

  /** Used by ReduceKeyPartitioner to determine the hash partition. */
  @transient var partitionCode = 0

  def createDeepCopy(): ReduceKeyMapSide = {
    val writable = new BytesWritable
    writable.set(bytesWritable.getBytes, 0, bytesWritable.getLength)
    val copy = new ReduceKeyMapSide(writable)
    copy.partitionCode = partitionCode
    copy
  }

  override def length: Int = bytesWritable.getLength()

  override def byteArray: Array[Byte] = bytesWritable.getBytes()

  override def equals(other: Any): Boolean  = {
    other match {
      case other: ReduceKey => {
        if (length != other.length) {
          false
        } else {
          WritableComparator.compareBytes(
            byteArray, 0, length, other.byteArray, 0, other.length) == 0
        }
      }
      case _ => false
    }
  }

  override def compareTo(that: ReduceKey): Int = {
    bytesWritable.compareTo(that.asInstanceOf[ReduceKeyMapSide].bytesWritable)
  }

  override def hashCode = bytesWritable.hashCode()

  override def toString = {
    this.getClass.getName + "(" + partitionCode + ": " + bytesWritable.toString + ")"
  }

  override def writeExternal(out: ObjectOutput) {
    bytesWritable.write(out)
  }

  override def readExternal(in: ObjectInput) {
    bytesWritable = new BytesWritable
    bytesWritable.readFields(in)
  }
}


class ReduceKeyReduceSide(private val _byteArray: Array[Byte])
  extends ReduceKey
  with Serializable {
  // This is serializable because of the possible external spill.

  def this() = this(null)

  override def byteArray: Array[Byte] = _byteArray

  override def length: Int = byteArray.length

  override def equals(other: Any): Boolean = {
    other match {
      case that: ReduceKeyReduceSide => {
        (this.byteArray.length == that.byteArray.length) && (this.compareTo(that) == 0)
      }
      case _ => false
    }
  }

  override def compareTo(that: ReduceKey): Int = {
    ReduceKeyReduceSide.comparator.compare(this.byteArray, that.byteArray)
  }

  override def hashCode = WritableComparator.hashBytes(_byteArray, _byteArray.length)

  override def toString = {
    this.getClass.getName + "(" + new BytesWritable(byteArray).toString + ")"
  }
}


object ReduceKeyReduceSide {
  val comparator = UnsignedBytes.lexicographicalComparator()
}


/**
 * A special Spark partitioner that allows hash partitioning of data based on
 * the partitionCode field in ReduceKey.
 */
class ReduceKeyPartitioner(partitions: Int) extends HashPartitioner(partitions) {

  override def getPartition(key: Any): Int = {
    key match {
      case k: ReduceKeyMapSide => {
        val mod = k.partitionCode % partitions
        if (mod < 0) mod + partitions else mod  // Guard against negative hash codes.
      }
      case other => {
        throw new Exception("ReduceKeyPartitioner expects object of class ReduceKeyMapSide, " +
          "but got " + other.getClass.getName)
      }
    }
  }
}
