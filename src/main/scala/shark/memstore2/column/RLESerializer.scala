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

package shark.memstore2.column

import shark.memstore2.buffer.ByteBufferReader

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.mutable.ListBuffer

import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList

import scala.collection.mutable._

import scala.annotation.tailrec


/* Decorator attempt with abstract override - incomplete - nandu
 * cannot get factory to work
trait RLEncoding[T] extends ColumnBuilder[T]{
  private var list:ListBuffer[T] = null

  abstract override def append(v: T) {
    list += v
    super.append(v)
  }

  abstract override def build: ByteBuffer {
    var rle = RLESerializer.encode(list.toList)
    var buf = ByteBuffer.allocate(20480)
    buf.order(ByteOrder.nativeOrder())

    RLESerializer.writeToBuffer(buf, l)
    buf.rewind

    super.buildIntoExistingBuffer


    buf
  }
}
*/

class RLEStreamingSerializer[A](val fac: () => A){
  var coded = collection.mutable.ListBuffer.empty[(Int,A)]
  private var coded_ = coded

  // TODO consider a encodeFinished() call and make this a read-only method
  def getCoded = {
    if(previousRun == 0) { // never initialized
      Nil
    }
    if(coded.length == 0 && previousRun != 0) { // exactly one value
      coded.append((previousRun, previous))
      coded.toList
    } else {
      val (lastrun, lastval): (Int, A) = coded.toList(coded.length-1)
      // println(" last " + lastval + " prev " + previous)
      if(lastval != previous) {
        coded.append((previousRun, previous))
      } else {
        if(lastrun != previousRun) {
          coded.insert(coded.length-1, (lastrun, lastval))
          previous = lastval
        }
      }
      coded.toList
    }
  }

  private var previous: A = fac()
  private var previousRun: Int = 0

  def encodeSingle(current: A): Unit =
    if (previousRun > 0) {
      if(previous == current) { // same value
        previousRun += 1
      } else { // diff value
        coded.append((previousRun, previous))
        previous = current
        previousRun = 1
      }
    } else { // first time
      previous = current
      previousRun = 1
    }

}

object RLESerializer{

// http://aperiodic.net/phil/scala/s-99/p13.scala
//     Implement the so-called run-length encoding data compression method
//
//     Example:
//     scala> encodeDirect(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e))
//     res0: List[(Int, Symbol)] = List((4,'a), (1,'b), (2,'c), (2,'a), (1,'d), (4,'e))

  // @tailrec def encode[A](ls: List[A]): List[(Int, A)] =
  //   if (ls.isEmpty) Nil
  //   else {
  //     val (packed, next) = ls span { _ == ls.head }
  //     (packed.length, packed.head) :: encode(next)
  //   }

  def encode[A](ls: List[A]): List[(Int, A)] =
    encode(ls, Nil)


  @tailrec def encode[A](ls: List[A], acc: List[(Int, A)]): List[(Int, A)] =
    if (ls.isEmpty) acc
    else {
      val (packed, next) = ls span { _ == ls.head }
      val revAcc = acc ::: List[(Int, A)]((packed.length, packed.head))
      encode(next, revAcc)
    }


// http://aperiodic.net/phil/scala/s-99/p12.scala
//     Given a run-length code list generated as specified in problem P10,
//     construct its uncompressed version.
//
//     Example:
//     scala> decode(List((4, 'a), (1, 'b), (2, 'c), (2, 'a), (1, 'd), (4, 'e)))
//     res0: List[Symbol] = List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)

  def decode[A](ls: List[(Int, A)]): List[A] =
    ls flatMap { e => List.fill(e._1)(e._2) }

  // Prefix the run lengths into the buffer
  // Also advances the buffer to the point after the lengths have been written
  def writeToBuffer(buf: ByteBuffer, lengths: List[Int]) {
    buf.order(ByteOrder.nativeOrder())
    val len = lengths.size
    val iter = lengths.iterator
    buf.putInt(len)
    while (iter.hasNext) {
      buf.putInt(iter.next)
    }
    buf
  }


  // Version with IntArrayList java implementation instead of scala Lists
  // Also advances the buffer to the point after the lengths have been written
  def writeToBuffer(buf: ByteBuffer, lengths: IntArrayList) {
    buf.order(ByteOrder.nativeOrder())
    buf.putInt(lengths.size)
    var i = 0
    while (i < lengths.size) {
      buf.putInt(lengths.get(i))
      i += 1
    }
    buf
  }


  // Create a List[Ints] representing the lengths from the byte buffer.
  // Also advances the buffer to the point after the lengths have been read
  // def readFromBuffer(buf: ByteBufferReader): List[Int] = {
  //   val numLengths = buf.getInt()
  //   var lengths = ListBuffer[Int]()
  //   var i = 0
  //   while (i < numLengths) {
  //     lengths += buf.getInt()
  //     i += 1
  //   }
  //   lengths.toList
  // }


  // Create a List[Ints] representing the lengths from the byte buffer.
  // Also advances the buffer to the point after the lengths have been read
  def readFromBuffer(buf: ByteBufferReader): IntArrayList = {
    val numLengths = buf.getInt()
    var lengths: IntArrayList = new IntArrayList(numLengths)

    var i = 0
    while (i < numLengths) {
      lengths.add(buf.getInt())
      i += 1
    }
    lengths
  }

} // object
