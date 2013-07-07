package shark.util

import java.lang.Integer.{ rotateLeft => rotl }
import scala.math._
import com.google.common.primitives.Ints

sealed class HashState(var h1: Int, var h2: Int, var h3: Int, var h4: Int) {
  
  val C1 = 0x239b961b
  val C2 = 0xab0e9789
  val C3 = 0x38b34ae5 
  val C4 = 0xa1e38b93

  @inline final def blockMix(k1: Int, k2: Int, k3: Int, k4: Int) {
    h1 ^= selfMixK1(k1)
    h1 = rotl(h1, 19); h1 += h2; h1 = h1 * 5 + 0x561ccd1b
    h2 ^= selfMixK2(k2)
    h2 = rotl(h2, 17); h2 += h3; h2 = h2 * 5 + 0x0bcaa747
    h3 ^= selfMixK3(k3)
    h3 = rotl(h3, 15); h3 += h4; h3 = h3 * 5 + 0x96cd1c35
    h4 ^= selfMixK4(k4)
    h4 = rotl(h4, 13); h4 += h1; h4 = h4 * 5 + 0x32ac3b17
  }

  @inline final def finalMix(k1: Int, k2: Int, k3: Int, k4: Int, len: Int) {
    h1 ^= (if (k1 ==0) 0 else selfMixK1(k1))
    h2 ^= (if (k2 ==0) 0 else selfMixK2(k2))
    h3 ^= (if (k3 ==0) 0 else selfMixK3(k3))
    h4 ^= (if (k4 ==0) 0 else selfMixK4(k4))
    h1 ^= len; h2 ^= len; h3 ^= len; h4 ^= len

    h1 += h2; h1 += h3; h1 += h4
    h2 += h1; h3 += h1; h4 += h1

    h1 = fmix(h1)
    h2 = fmix(h2)
    h3 = fmix(h3)
    h4 = fmix(h4)

    h1 += h2; h1 += h3; h1 += h4
    h2 += h1; h3 += h1; h4 += h1
  }

  @inline final def fmix(hash: Int): Int = {
    var h = hash
    h ^= h >> 16
    h *= 0x85ebca6b
    h ^= h >> 13
    h *= 0xc2b2ae35
    h ^= h >> 16
    h
  }
  @inline final def selfMixK1(k: Int): Int = {
    var k1 = k; k1 *= C1; k1 = rotl(k1, 15); k1 *= C2
    k1
  }

  @inline final def selfMixK2(k: Int): Int = {
    var k2 = k; k2 *= C2; k2 = rotl(k2, 16); k2 *= C3
    k2
  }

  @inline final def selfMixK3(k: Int): Int = {
    var k3 = k; k3 *= C3; k3 = rotl(k3, 17); k3 *= C4
    k3
  }

  @inline final def selfMixK4(k: Int): Int = {
    var k4 = k; k4 *= C4; k4 = rotl(k4, 18); k4 *= C1
    k4
  }
}

object MurmurHash3_x86_128 extends AnyRef with Serializable {

  @inline final def hash(data: Array[Byte], seed: Int)
  : Tuple4[Int, Int, Int, Int]  = {
    hash(data, seed, data.length)
  }

  @inline final def hash(data: Array[Byte], seed: Int, length: Int)
  : Tuple4[Int, Int, Int, Int] = {
    var i = 0
    val blocks = length >> 4
    val state = new HashState(seed, seed, seed, seed)
    while (i < blocks) {
      val k1 = getInt(data, 4*i, 4)
      val k2 = getInt(data, 4*i + 4, 4)
      val k3 = getInt(data, 4*i + 8, 4)
      val k4 = getInt(data, 4*i + 12, 4)
      state.blockMix(k1, k2, k3, k4)
      i += 1
    }
    var k1 , k2, k3, k4 = 0
    val tail = blocks*16
	val rem = length - tail
    // atmost 15 bytes remain
	rem match {
      case 12 | 13 | 14 | 15 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, 4)
        k3 = getInt(data, tail + 8, 4)
        k4 = getInt(data, tail + 12, rem - 12)
      }
      case 8 | 9 | 10 | 11 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, 4)
        k3 = getInt(data, tail + 8, rem - 8)
      }
      case 4 | 5 | 6 | 7 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, rem - 4)
      }
      case 0 | 1 | 2 | 3 => {
        k1 = getInt(data, tail, rem)
      }
    }
    state.finalMix(k1, k2, k3, k4, length)
    (state.h1, state.h2, state.h3, state.h4)
  }
  
  @inline final def getInt(data: Array[Byte], index: Int, rem: Int): Int = {
    rem  match {
      case 3 => data(index) << 24 | 
      			(data(index + 1) & 0xFF) << 16 |
      			(data(index + 2) & 0xFF) << 8 
      case 2 => data(index) << 24 | 
      			(data(index + 1) & 0xFF) << 16
      case 1 => data(index) << 24
      case 0 => 0
      case _ => data(index) << 24 | 
      			(data(index + 1) & 0xFF) << 16 |
      			(data(index + 2) & 0xFF) << 8 |
      			(data(index + 3) & 0xFF)
    }
  }
}