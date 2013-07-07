package shark.util

import java.util.BitSet
import java.nio.charset.Charset
import scala.math._
import com.google.common.primitives.Bytes
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs

class BloomFilter(numBitsPerElement: Double, expectedSize: Int, numHashes: Int) 
	extends AnyRef with Serializable{

  val SEED = System.getProperty("shark.bloomfilter.seed","1234567890").toInt
  val bitSetSize = ceil(numBitsPerElement * expectedSize).toInt
  val bitSet = new BitSet(bitSetSize)

  def this(fpp: Double, expectedSize: Int) {
   this(BloomFilter.numBits(fpp, expectedSize), 
       expectedSize, 
       BloomFilter.numHashes(fpp, expectedSize))
  }

  def add(data: Array[Byte]) {
    val hashes = hash(data, numHashes)
    var i = hashes.size
    while (i > 0) {
      i -= 1
      bitSet.set(hashes(i) % bitSetSize, true)
    }
  }

  def add(data: Array[Byte], len: Int) {
    val hashes = hash(data, numHashes, len)
    var i = hashes.size
    while (i > 0) {
      i -= 1
      bitSet.set(hashes(i) % bitSetSize, true)
    }
  }

  def add(data: String, charset: Charset=Charset.forName("UTF-8")) {
    add(data.getBytes(charset))
  }

  def add(data: Int) {
    add(Ints.toByteArray(data))
  }
  
  def add(data: Long) {
    add(Longs.toByteArray(data))
  }

  def contains(data: String, charset: Charset=Charset.forName("UTF-8")): Boolean = {
    contains(data.getBytes(charset))
  }

  def contains(data: Int): Boolean = {
    contains(Ints.toByteArray(data))
  }
  
  def contains(data: Long): Boolean = {
    contains(Longs.toByteArray(data))
  }

  def contains(data: Array[Byte]): Boolean = {
    !hash(data,numHashes).exists {
      h => !bitSet.get(h % bitSetSize)
    } 
  }
  
  def contains(data: Array[Byte], len: Int): Boolean = {
    !hash(data,numHashes, len).exists {
      h => !bitSet.get(h % bitSetSize)
    } 
  }

  private def hash(data: Array[Byte], n: Int): Seq[Int] = {
    hash(data, n, data.length)
  }

  private def hash(data: Array[Byte], n: Int, len: Int): Seq[Int] = {
    val s = n >> 2
    val a = new Array[Int](n)
    var i = 0
    while (i < s) {
      val u = MurmurHash3_x86_128.hash(data, SEED + i, len)
      a(i) = u._1.abs
      var j = i + 1
      if (j < n)
        a(j) = u._2.abs
      j += 1
      if (j < n)
        a(j) = u._3.abs
      j += 1
      if (j < n)
        a(j) = u._4.abs
      i += 1
    }
    a
  }
}

object BloomFilter {
  
  def numBits(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))) / log(2)
  
  def numHashes(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))).toInt
}