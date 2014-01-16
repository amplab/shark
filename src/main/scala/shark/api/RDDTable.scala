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

package shark.api

// *** This file is auto-generated from RDDTable_generator.py ***
import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object RDDTableImplicits {
  private type C[T] = ClassTag[T]


  implicit def rddToTable2[T1: C, T2: C]
  (rdd: RDD[(T1, T2)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable3[T1: C, T2: C, T3: C]
  (rdd: RDD[(T1, T2, T3)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable4[T1: C, T2: C, T3: C, T4: C]
  (rdd: RDD[(T1, T2, T3, T4)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable5[T1: C, T2: C, T3: C, T4: C, T5: C]
  (rdd: RDD[(T1, T2, T3, T4, T5)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable6[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable7[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable8[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable9[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable10[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable11[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable12[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable13[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable14[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable15[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable16[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable17[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable18[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable19[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable20[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C,
  T20: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable21[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C,
  T20: C, T21: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable22[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C,
  T20: C, T21: C, T22: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22)]): RDDTableFunctions = RDDTable(rdd)


}

object RDDTable {

  private type C[T] = ClassTag[T]
  private def ct[T](implicit c: ClassTag[T]) = c

  def apply[T1: C, T2: C]
  (rdd: RDD[(T1, T2)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2]))
  }


  def apply[T1: C, T2: C, T3: C]
  (rdd: RDD[(T1, T2, T3)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C]
  (rdd: RDD[(T1, T2, T3, T4)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C]
  (rdd: RDD[(T1, T2, T3, T4, T5)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16], ct[T17]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16], ct[T17],
      ct[T18]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16], ct[T17],
      ct[T18], ct[T19]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C,
  T20: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16], ct[T17],
      ct[T18], ct[T19], ct[T20]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C,
  T20: C, T21: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16], ct[T17],
      ct[T18], ct[T19], ct[T20], ct[T21]))
  }


  def apply[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C,
  T11: C, T12: C, T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C,
  T20: C, T21: C, T22: C]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9],
      ct[T10], ct[T11], ct[T12], ct[T13], ct[T14], ct[T15], ct[T16], ct[T17],
      ct[T18], ct[T19], ct[T20], ct[T21], ct[T22]))
  }

}
