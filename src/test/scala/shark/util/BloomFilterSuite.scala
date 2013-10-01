package shark.util

import org.scalatest.FunSuite

class BloomFilterSuite extends FunSuite{

  test("Integer") {
    val bf = new BloomFilter(0.03, 1000000)
    Range(0, 1000000).foreach {
      i => bf.add(i)
    }
    assert(bf.contains(333))
    assert(bf.contains(678))
    assert(!bf.contains(1200000))
  }
  
  test("Integer FP") {
    val bf = new BloomFilter(0.03,1000)
    Range(0,700).foreach {
      i => bf.add(i)
    }
    assert(bf.contains(333))
    assert(bf.contains(678))
    //is the fraction of false positives in line with what we expect ?
    val e = Range(0, 100).map {
      i => bf.contains(i*10)
    }
    val s = e.groupBy(x => x).map(x => (x._1, x._2.size))
    val t = s(true)
    val f = s(false)
    assert(f > 25 && f < 35)
    assert(t < 75 && t > 65)
    // expect false positive to be < 3 % and no false negatives
  }
}