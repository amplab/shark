
/*
 * Copyright (C) 2013 The Regents of The University California.
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



package shark.api

// *** This file is auto-generated from TableRDDGenerated_generator.py ***
import scala.language.implicitConversions
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition}

import scala.reflect.ClassTag

class TableSeqRDD(prev: TableRDD)
  extends RDD[Seq[Any]](prev) {

  def getSchema = prev.schema

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[Any]] = {
    prev.compute(split, context).map( row =>
      (0 until prev.schema.size).map(i => row.getPrimitive(i)) )
  }
}


class TableRDD1[T1](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple1[T1]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 1, "Table only has " + tableCols + " columns, expecting 1")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple1[T1]] = {
    prev.compute(split, context).map( row =>
      new Tuple1[T1](
                row.getPrimitiveGeneric[T1](0) ) )

  }
}

class TableRDD2[T1, T2](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple2[T1, T2]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 2, "Table only has " + tableCols + " columns, expecting 2")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple2[T1, T2]] = {
    prev.compute(split, context).map( row =>
      new Tuple2[T1, T2](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1) ) )

  }
}

class TableRDD3[T1, T2, T3](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple3[T1, T2, T3]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 3, "Table only has " + tableCols + " columns, expecting 3")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple3[T1, T2, T3]] = {
    prev.compute(split, context).map( row =>
      new Tuple3[T1, T2, T3](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2)
 ) )

  }
}

class TableRDD4[T1, T2, T3, T4](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple4[T1, T2, T3, T4]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 4, "Table only has " + tableCols + " columns, expecting 4")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple4[T1, T2, T3, T4]] = {
    prev.compute(split, context).map( row =>
      new Tuple4[T1, T2, T3, T4](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3) ) )

  }
}

class TableRDD5[T1, T2, T3, T4, T5](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple5[T1, T2, T3, T4, T5]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 5, "Table only has " + tableCols + " columns, expecting 5")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple5[T1, T2, T3, T4, T5]] = {
    prev.compute(split, context).map( row =>
      new Tuple5[T1, T2, T3, T4, T5](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4) ) )

  }
}

class TableRDD6[T1, T2, T3, T4, T5, T6](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple6[T1, T2, T3, T4, T5, T6]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 6, "Table only has " + tableCols + " columns, expecting 6")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple6[T1, T2, T3, T4, T5, T6]] = {
    prev.compute(split, context).map( row =>
      new Tuple6[T1, T2, T3, T4, T5, T6](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5)
 ) )

  }
}

class TableRDD7[T1, T2, T3, T4, T5, T6, T7](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple7[T1, T2, T3, T4, T5, T6, T7]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 7, "Table only has " + tableCols + " columns, expecting 7")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple7[T1, T2, T3, T4, T5, T6, T7]] = {
    prev.compute(split, context).map( row =>
      new Tuple7[T1, T2, T3, T4, T5, T6, T7](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6) ) )

  }
}

class TableRDD8[T1, T2, T3, T4, T5, T6, T7, T8](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 8, "Table only has " + tableCols + " columns, expecting 8")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]] = {
    prev.compute(split, context).map( row =>
      new Tuple8[T1, T2, T3, T4, T5, T6, T7, T8](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7) ) )

  }
}

class TableRDD9[T1, T2, T3, T4, T5, T6, T7, T8, T9](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 9, "Table only has " + tableCols + " columns, expecting 9")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = {
    prev.compute(split, context).map( row =>
      new Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8)
 ) )

  }
}

class TableRDD10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 10, "Table only has " + tableCols + " columns, expecting 10")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] = {
    prev.compute(split, context).map( row =>
      new Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9) ) )

  }
}

class TableRDD11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 11, "Table only has " + tableCols + " columns, expecting 11")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]] = {
    prev.compute(split, context).map( row =>
      new Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10) ) )

  }
}

class TableRDD12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 12, "Table only has " + tableCols + " columns, expecting 12")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]] = {
    prev.compute(split, context).map( row =>
      new Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11)
 ) )

  }
}

class TableRDD13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 13, "Table only has " + tableCols + " columns, expecting 13")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]] = {
    prev.compute(split, context).map( row =>
      new Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12) ) )

  }
}

class TableRDD14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 14, "Table only has " + tableCols + " columns, expecting 14")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]] = {
    prev.compute(split, context).map( row =>
      new Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13) ) )

  }
}

class TableRDD15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 15, "Table only has " + tableCols + " columns, expecting 15")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]] = {
    prev.compute(split, context).map( row =>
      new Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14)
 ) )

  }
}

class TableRDD16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 16, "Table only has " + tableCols + " columns, expecting 16")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]] = {
    prev.compute(split, context).map( row =>
      new Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15) ) )

  }
}

class TableRDD17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 17, "Table only has " + tableCols + " columns, expecting 17")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17]] = {
    prev.compute(split, context).map( row =>
      new Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15), row.getPrimitiveGeneric[T17](16) ) )

  }
}

class TableRDD18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 18, "Table only has " + tableCols + " columns, expecting 18")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18]] = {
    prev.compute(split, context).map( row =>
      new Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15), row.getPrimitiveGeneric[T17](16), row.getPrimitiveGeneric[T18](17)
 ) )

  }
}

class TableRDD19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 19, "Table only has " + tableCols + " columns, expecting 19")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19]] = {
    prev.compute(split, context).map( row =>
      new Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15), row.getPrimitiveGeneric[T17](16), row.getPrimitiveGeneric[T18](17),
        row.getPrimitiveGeneric[T19](18) ) )

  }
}

class TableRDD20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 20, "Table only has " + tableCols + " columns, expecting 20")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20]] = {
    prev.compute(split, context).map( row =>
      new Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15), row.getPrimitiveGeneric[T17](16), row.getPrimitiveGeneric[T18](17),
        row.getPrimitiveGeneric[T19](18), row.getPrimitiveGeneric[T20](19) ) )

  }
}

class TableRDD21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 21, "Table only has " + tableCols + " columns, expecting 21")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21]] = {
    prev.compute(split, context).map( row =>
      new Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15), row.getPrimitiveGeneric[T17](16), row.getPrimitiveGeneric[T18](17),
        row.getPrimitiveGeneric[T19](18), row.getPrimitiveGeneric[T20](19), row.getPrimitiveGeneric[T21](20)
 ) )

  }
}

class TableRDD22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == 22, "Table only has " + tableCols + " columns, expecting 22")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22]] = {
    prev.compute(split, context).map( row =>
      new Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22](
                row.getPrimitiveGeneric[T1](0), row.getPrimitiveGeneric[T2](1), row.getPrimitiveGeneric[T3](2),
        row.getPrimitiveGeneric[T4](3), row.getPrimitiveGeneric[T5](4), row.getPrimitiveGeneric[T6](5),
        row.getPrimitiveGeneric[T7](6), row.getPrimitiveGeneric[T8](7), row.getPrimitiveGeneric[T9](8),
        row.getPrimitiveGeneric[T10](9), row.getPrimitiveGeneric[T11](10), row.getPrimitiveGeneric[T12](11),
        row.getPrimitiveGeneric[T13](12), row.getPrimitiveGeneric[T14](13), row.getPrimitiveGeneric[T15](14),
        row.getPrimitiveGeneric[T16](15), row.getPrimitiveGeneric[T17](16), row.getPrimitiveGeneric[T18](17),
        row.getPrimitiveGeneric[T19](18), row.getPrimitiveGeneric[T20](19), row.getPrimitiveGeneric[T21](20),
        row.getPrimitiveGeneric[T22](21) ) )

  }
}
