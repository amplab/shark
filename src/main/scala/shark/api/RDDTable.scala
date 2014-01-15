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

// *** This file is auto-generated from rddtable_generator.py ***

import org.apache.spark.rdd.RDD

object RDDTableImplicits {
  private type M[T] = ClassManifest[T]


  implicit def rddToTable2[T1: M, T2: M]
  (rdd: RDD[(T1, T2)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable3[T1: M, T2: M, T3: M]
  (rdd: RDD[(T1, T2, T3)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable4[T1: M, T2: M, T3: M, T4: M]
  (rdd: RDD[(T1, T2, T3, T4)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable5[T1: M, T2: M, T3: M, T4: M, T5: M]
  (rdd: RDD[(T1, T2, T3, T4, T5)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable6[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable7[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable8[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable9[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable10[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable11[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable12[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable13[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable14[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable15[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable16[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable17[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable18[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable19[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable20[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M,
  T20: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable21[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M,
  T20: M, T21: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21)]): RDDTableFunctions = RDDTable(rdd)


  implicit def rddToTable22[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M,
  T20: M, T21: M, T22: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22)]): RDDTableFunctions = RDDTable(rdd)


}

object RDDTable {

  private type M[T] = ClassManifest[T]
  private def m[T](implicit m : ClassManifest[T]) = classManifest[T](m)

  def apply[T1: M, T2: M]
  (rdd: RDD[(T1, T2)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2]))
  }


  def apply[T1: M, T2: M, T3: M]
  (rdd: RDD[(T1, T2, T3)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M]
  (rdd: RDD[(T1, T2, T3, T4)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M]
  (rdd: RDD[(T1, T2, T3, T4, T5)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16], m[T17]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18], m[T19]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M,
  T20: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18], m[T19],
      m[T20]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M,
  T20: M, T21: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18], m[T19],
      m[T20], m[T21]))
  }


  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M,
  T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M, T19: M,
  T20: M, T21: M, T22: M]
  (rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22)]) = {
    val cm = implicitly[Manifest[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(cm)
    new RDDTableFunctions(rddSeq, Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10],
      m[T11], m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18], m[T19],
      m[T20], m[T21], m[T22]))
  }

}
