package shark

import org.apache.hadoop.hive.ql.exec.MapSplitPruning._
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

class MapSplitPruning extends FunSuite {

  def T(str: String): Text = new Text(str)

  test("test different data types") {
    assert(testEqual(1, 2, true))
    assert(testGreaterThan(1, 2, true))
    assert(testEqualOrGreaterThan(1, 2, true))
    assert(testLessThan(1, 2, true))
    assert(testEqualOrLessThan(1, 2, true))
    assert(testNotEqual(1, 2, true))
  }
  
  test("testEqual") {
    assert(testEqual(1, 20, 2) == true)
    assert(testEqual(1, 20, 1) == true)
    assert(testEqual(1, 1, 1) == true)
    assert(testEqual(1, 20, 0) == false)
    assert(testEqual(1, 20, 21) == false)

    assert(testEqual(1.0, 20.0, 2.0) == true)
    assert(testEqual(1.0, 20.0, 1.0) == true)
    assert(testEqual(1.0, 1.0, 1.0) == true)
    assert(testEqual(1.0, 20.0, 0) == false)
    assert(testEqual(1.0, 20.0, 21.0) == false)

    assert(testEqual(false, true, true) == true)
    assert(testEqual(true, true, true) == true)
    assert(testEqual(false, false, false) == true)
    assert(testEqual(false, false, true) == false)
    assert(testEqual(false, false, true) == false)

    assert(testEqual(T("1"), T("3"), T("2")) == true)
    assert(testEqual(T("1"), T("3"), T("1")) == true)
    assert(testEqual(T("1"), T("1"), T("1")) == true)
    assert(testEqual(T("1"), T("3"), T("0")) == false)
    assert(testEqual(T("1"), T("3"), T("4")) == false)
    assert(testEqual(T("1"), T("3"), "2") == true)
    assert(testEqual(T("1"), T("3"), "1") == true)
    assert(testEqual(T("1"), T("1"), "1") == true)
    assert(testEqual(T("1"), T("3"), "0") == false)
    assert(testEqual(T("1"), T("3"), "4") == false)
  }

  test("testGreaterThan") {
    assert(testGreaterThan(1, 20, 2) == true)
    assert(testGreaterThan(1, 20, 1) == true)
    assert(testGreaterThan(1, 20, 0) == true)
    assert(testGreaterThan(1, 20, 20) == false)
    assert(testGreaterThan(1, 20, 21) == false)

    assert(testGreaterThan(1.0, 20.0, 2) == true)
    assert(testGreaterThan(1.0, 20.0, 1) == true)
    assert(testGreaterThan(1.0, 20.0, 0) == true)
    assert(testGreaterThan(1.0, 20.0, 20) == false)
    assert(testGreaterThan(1.0, 20.0, 21) == false)

    assert(testGreaterThan(T("1"), T("3"), T("2")) == true)
    assert(testGreaterThan(T("1"), T("3"), T("1")) == true)
    assert(testGreaterThan(T("1"), T("1"), T("1")) == false)
    assert(testGreaterThan(T("1"), T("3"), T("0")) == true)
    assert(testGreaterThan(T("1"), T("3"), T("4")) == false)
    assert(testGreaterThan(T("1"), T("3"), "2") == true)
    assert(testGreaterThan(T("1"), T("3"), "1") == true)
    assert(testGreaterThan(T("1"), T("1"), "1") == false)
    assert(testGreaterThan(T("1"), T("3"), "0") == true)
    assert(testGreaterThan(T("1"), T("3"), "4") == false)
  }

  test("testEqualOrGreaterThan") {
    assert(testEqualOrGreaterThan(1, 20, 2) == true)
    assert(testEqualOrGreaterThan(1, 20, 1) == true)
    assert(testEqualOrGreaterThan(1, 20, 0) == true)
    assert(testEqualOrGreaterThan(1, 20, 20) == true)
    assert(testEqualOrGreaterThan(1, 20, 21) == false)

    assert(testEqualOrGreaterThan(1.0, 20.0, 2) == true)
    assert(testEqualOrGreaterThan(1.0, 20.0, 1) == true)
    assert(testEqualOrGreaterThan(1.0, 20.0, 0) == true)
    assert(testEqualOrGreaterThan(1.0, 20.0, 20) == true)
    assert(testEqualOrGreaterThan(1.0, 20.0, 21) == false)

    assert(testEqualOrGreaterThan(T("1"), T("3"), T("2")) == true)
    assert(testEqualOrGreaterThan(T("1"), T("3"), T("1")) == true)
    assert(testEqualOrGreaterThan(T("1"), T("1"), T("1")) == true)
    assert(testEqualOrGreaterThan(T("1"), T("3"), T("0")) == true)
    assert(testEqualOrGreaterThan(T("1"), T("3"), T("4")) == false)
    assert(testEqualOrGreaterThan(T("1"), T("3"), "2") == true)
    assert(testEqualOrGreaterThan(T("1"), T("3"), "1") == true)
    assert(testEqualOrGreaterThan(T("1"), T("1"), "1") == true)
    assert(testEqualOrGreaterThan(T("1"), T("3"), "0") == true)
    assert(testEqualOrGreaterThan(T("1"), T("3"), "4") == false)
  }

  test("testLessThan") {
    assert(testLessThan(1, 20, 2) == true)
    assert(testLessThan(1, 20, 1) == false)
    assert(testLessThan(1, 20, 0) == false)
    assert(testLessThan(1, 20, 20) == true)
    assert(testLessThan(1, 20, 21) == true)

    assert(testLessThan(1.0, 20.0, 2) == true)
    assert(testLessThan(1.0, 20.0, 1) == false)
    assert(testLessThan(1.0, 20.0, 0) == false)
    assert(testLessThan(1.0, 20.0, 20) == true)
    assert(testLessThan(1.0, 20.0, 21) == true)

    assert(testLessThan(T("1"), T("3"), T("2")) == true)
    assert(testLessThan(T("1"), T("3"), T("1")) == false)
    assert(testLessThan(T("1"), T("1"), T("1")) == false)
    assert(testLessThan(T("1"), T("3"), T("0")) == false)
    assert(testLessThan(T("1"), T("3"), T("4")) == true)
    assert(testLessThan(T("1"), T("3"), "2") == true)
    assert(testLessThan(T("1"), T("3"), "1") == false)
    assert(testLessThan(T("1"), T("1"), "1") == false)
    assert(testLessThan(T("1"), T("3"), "0") == false)
    assert(testLessThan(T("1"), T("3"), "4") == true)
  }

  test("testEqualOrLessThan") {
    assert(testEqualOrLessThan(1, 20, 2) == true)
    assert(testEqualOrLessThan(1, 20, 1) == true)
    assert(testEqualOrLessThan(1, 20, 0) == false)
    assert(testEqualOrLessThan(1, 20, 20) == true)
    assert(testEqualOrLessThan(1, 20, 21) == true)

    assert(testEqualOrLessThan(1.0, 20.0, 2) == true)
    assert(testEqualOrLessThan(1.0, 20.0, 1) == true)
    assert(testEqualOrLessThan(1.0, 20.0, 0) == false)
    assert(testEqualOrLessThan(1.0, 20.0, 20) == true)
    assert(testEqualOrLessThan(1.0, 20.0, 21) == true)

    assert(testEqualOrLessThan(T("1"), T("3"), T("2")) == true)
    assert(testEqualOrLessThan(T("1"), T("3"), T("1")) == true)
    assert(testEqualOrLessThan(T("1"), T("1"), T("1")) == true)
    assert(testEqualOrLessThan(T("1"), T("3"), T("0")) == false)
    assert(testEqualOrLessThan(T("1"), T("3"), T("4")) == true)
    assert(testEqualOrLessThan(T("1"), T("3"), "2") == true)
    assert(testEqualOrLessThan(T("1"), T("3"), "1") == true)
    assert(testEqualOrLessThan(T("1"), T("1"), "1") == true)
    assert(testEqualOrLessThan(T("1"), T("3"), "0") == false)
    assert(testEqualOrLessThan(T("1"), T("3"), "4") == true)
  }

  test("testNotEqual") {
    assert(testNotEqual(1, 20, 2) == true)
    assert(testNotEqual(1, 20, 1) == true)
    assert(testNotEqual(1, 1, 1) == false)
    assert(testNotEqual(1, 20, 0) == true)
    assert(testNotEqual(1, 20, 21) == true)

    assert(testNotEqual(1.0, 20.0, 2.0) == true)
    assert(testNotEqual(1.0, 20.0, 1.0) == true)
    assert(testNotEqual(1.0, 1.0, 1.0) == false)
    assert(testNotEqual(1.0, 20.0, 0) == true)
    assert(testNotEqual(1.0, 20.0, 21.0) == true)

    assert(testNotEqual(false, true, true) == true)
    assert(testNotEqual(true, true, true) == false)
    assert(testNotEqual(false, false, false) == false)
    assert(testNotEqual(false, false, true) == true)
    assert(testNotEqual(false, false, true) == true)

    assert(testNotEqual(T("1"), T("3"), T("2")) == true)
    assert(testNotEqual(T("1"), T("3"), T("1")) == true)
    assert(testNotEqual(T("1"), T("1"), T("1")) == false)
    assert(testNotEqual(T("1"), T("3"), T("0")) == true)
    assert(testNotEqual(T("1"), T("3"), T("4")) == true)
    assert(testNotEqual(T("1"), T("3"), "2") == true)
    assert(testNotEqual(T("1"), T("3"), "1") == true)
    assert(testNotEqual(T("1"), T("1"), "1") == false)
    assert(testNotEqual(T("1"), T("3"), "0") == true)
    assert(testNotEqual(T("1"), T("3"), "4") == true)
  }

}
