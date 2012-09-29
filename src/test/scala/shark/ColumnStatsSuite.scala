package shark

import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import shark.memstore._

class ColumnStatsSuite extends FunSuite {

  test("ColumnStats no stats") {
    // Make sure they are of the right type. No errors thrown.
    (new ColumnStats.BooleanColumnNoStats).add(false)
    (new ColumnStats.ByteColumnNoStats).add(1.toByte)
    (new ColumnStats.ShortColumnNoStats).add(1.toShort)
    (new ColumnStats.IntColumnNoStats).add(14324)
    (new ColumnStats.LongColumnNoStats).add(3452435L)
    (new ColumnStats.FloatColumnNoStats).add(1.025F)
    (new ColumnStats.DoubleColumnNoStats).add(1.2143)
  }

  test("ColumnStats.BooleanColumnStats") {
    var c = new ColumnStats.BooleanColumnStats
    c.add(false)
    assert(c.min == false && c.max == false)
    c.add(false)
    assert(c.min == false && c.max == false)
    c.add(true)
    assert(c.min == false && c.max == true)

    c = new ColumnStats.BooleanColumnStats
    c.add(true)
    assert(c.min == true && c.max == true)
    c.add(false)
    assert(c.min == false && c.max == true)
  }

  test("ColumnStats.ByteColumnStats") {
    var c = new ColumnStats.ByteColumnStats
    c.add(0)
    assert(c.min == 0 && c.max == 0)
    c.add(1)
    assert(c.min == 0 && c.max == 1)
    c.add(-1)
    assert(c.min == -1 && c.max == 1)
    c.add(2)
    assert(c.min == -1 && c.max == 2)
    c.add(-2)
    assert(c.min == -2 && c.max == 2)
  }

  test("ColumnStats.ShortColumnStats") {
    var c = new ColumnStats.ShortColumnStats
    c.add(0)
    assert(c.min == 0 && c.max == 0)
    c.add(1)
    assert(c.min == 0 && c.max == 1)
    c.add(-1)
    assert(c.min == -1 && c.max == 1)
    c.add(1024)
    assert(c.min == -1 && c.max == 1024)
    c.add(-1024)
    assert(c.min == -1024 && c.max == 1024)
  }

  test("ColumnStats.IntColumnStats") {
    var c = new ColumnStats.IntColumnStats
    c.add(0)
    assert(c.min == 0 && c.max == 0)
    c.add(1)
    assert(c.min == 0 && c.max == 1)
    c.add(-1)
    assert(c.min == -1 && c.max == 1)
    c.add(65537)
    assert(c.min == -1 && c.max == 65537)
    c.add(-65537)
    assert(c.min == -65537 && c.max == 65537)

    c = new ColumnStats.IntColumnStats
    assert(c.isOrdered && c.isAscending && c.isDescending)
    assert(c.maxDelta == 0)

    c = new ColumnStats.IntColumnStats
    Array(1).foreach(c.add)
    assert(c.isOrdered && c.isAscending && c.isDescending)
    assert(c.maxDelta == 0)

    c = new ColumnStats.IntColumnStats
    Array(1, 2, 3, 3, 4, 22).foreach(c.add)
    assert(c.isOrdered && c.isAscending && !c.isDescending)
    assert(c.maxDelta == 18)

    c = new ColumnStats.IntColumnStats
    Array(22, 1, 0, -5).foreach(c.add)
    assert(c.isOrdered && !c.isAscending && c.isDescending)
    assert(c.maxDelta == 21)

    c = new ColumnStats.IntColumnStats
    Array(22, 1, 24).foreach(c.add)
    assert(!c.isOrdered && !c.isAscending && !c.isDescending)
  }

  test("ColumnStats.LongColumnStats") {
    var c = new ColumnStats.LongColumnStats
    c.add(0)
    assert(c.min == 0 && c.max == 0)
    c.add(1)
    assert(c.min == 0 && c.max == 1)
    c.add(-1)
    assert(c.min == -1 && c.max == 1)
    c.add(Int.MaxValue.toLong + 1L)
    assert(c.min == -1 && c.max == Int.MaxValue.toLong + 1L)
    c.add(Int.MinValue.toLong - 1L)
    assert(c.min == Int.MinValue.toLong - 1L && c.max == Int.MaxValue.toLong + 1L)
  }

  test("ColumnStats.FloatColumnStats") {
    var c = new ColumnStats.FloatColumnStats
    c.add(0)
    assert(c.min == 0 && c.max == 0)
    c.add(1)
    assert(c.min == 0 && c.max == 1)
    c.add(-1)
    assert(c.min == -1 && c.max == 1)
    c.add(20.5445F)
    assert(c.min == -1 && c.max == 20.5445F)
    c.add(-20.5445F)
    assert(c.min == -20.5445F && c.max == 20.5445F)
  }

  test("ColumnStats.DoubleColumnStats") {
    var c = new ColumnStats.DoubleColumnStats
    c.add(0)
    assert(c.min == 0 && c.max == 0)
    c.add(1)
    assert(c.min == 0 && c.max == 1)
    c.add(-1)
    assert(c.min == -1 && c.max == 1)
    c.add(20.5445)
    assert(c.min == -1 && c.max == 20.5445)
    c.add(-20.5445)
    assert(c.min == -20.5445 && c.max == 20.5445)
  }

  test("ColumnStats.TextColumnStats") {
    implicit def T(str: String): Text = new Text(str)
    var c = new ColumnStats.TextColumnStats
    assert(c.min == null && c.max == null)
    c.add("a")
    assert(c.min.equals(T("a")) && c.max.equals(T("a")))
    c.add("b")
    assert(c.min.equals(T("a")) && c.max.equals(T("b")))
    c.add("b")
    assert(c.min.equals(T("a")) && c.max.equals(T("b")))
    c.add("cccc")
    assert(c.min.equals(T("a")) && c.max.equals(T("cccc")))
    c.add("0987")
    assert(c.min.equals(T("0987")) && c.max.equals(T("cccc")))
  }
}
