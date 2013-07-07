package shark.util

import java.util.BitSet
import java.nio.charset.Charset
import scala.math._
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import scala.collection.mutable.ArrayBuffer
import com.google.common.hash.Murmur3_128HashFunction
import com.google.common.hash.Hashing
import com.google.common.primitives.Bytes

class BloomFilter(numBitsPerElement: Double, expectedSize: Int, numHashes: Int) 
	extends AnyRef with Serializable{

  val SEED = System.getProperty("shark.bloomfilter.seed","123456789").toLong
  val bitSetSize = ceil(numBitsPerElement * expectedSize).toInt
  val bitSet = new BitSet(bitSetSize)

  def this(fpp: Double, expectedSize: Int) {
   this(BloomFilter.numBits(fpp, expectedSize), 
       expectedSize, 
       BloomFilter.numHashes(fpp, expectedSize))
  }
  
  def add(data: Array[Byte]) {
    hash(data,numHashes).foreach {
      h => bitSet.set(h % bitSetSize, true)
    }
  }
  
  def add(data: Array[Byte], len: Int) {
    hash(data,numHashes, len).foreach {
      h => bitSet.set(h % bitSetSize, true)
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
    val s = (ceil (n / 4.0)).toInt
    val l = 4 * s
    val a = new Array[Int](n)
    Range(0, s).foreach {
      i => {
        val u = MurmurHash3.hash(data, SEED + i, len)
        a(i) = u(0).toInt.abs
        var j = i + 1
        if (j < n)
        a(j) = (u(0) >> 32).toInt.abs
        j += 1
        if (j < n)
        a(j) = u(1).toInt.abs
        j += 1
        if (j < n)
        a(j) = (u(1) >> 32).toInt.abs
      }
    }
    a
  }
}

object BloomFilter {
  
  def numBits(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))) / log(2)
  
  def numHashes(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))).toInt
}