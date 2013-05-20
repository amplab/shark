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

package shark.memstore2

import java.sql.Timestamp

import org.apache.hadoop.io.Text

import org.scalatest.FunSuite

import shark.memstore2.column.ColumnStats


class ColumnStatsSuite extends FunSuite {

  test("BooleanColumnStats") {
    var c = new ColumnStats.BooleanColumnStats
    c.append(false)
    assert(c.min == false && c.max == false)
    c.append(false)
    assert(c.min == false && c.max == false)
    c.append(true)
    assert(c.min == false && c.max == true)

    c = new ColumnStats.BooleanColumnStats
    c.append(true)
    c.appendNull()
    assert(c.min == true && c.max == true)
    c.appendNull()
    c.append(false)
    assert(c.min == false && c.max == true)
    c.appendNull()
    assert(c.nullCount == 3)
  }

  test("ByteColumnStats") {
    var c = new ColumnStats.ByteColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    c.appendNull()
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(2)
    assert(c.min == -1 && c.max == 2)
    c.append(-2)
    assert(c.min == -2 && c.max == 2)
    assert(c.nullCount == 1)
  }

  test("ShortColumnStats") {
    var c = new ColumnStats.ShortColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    c.appendNull()
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(1024)
    assert(c.min == -1 && c.max == 1024)
    c.append(-1024)
    assert(c.min == -1024 && c.max == 1024)
    assert(c.nullCount == 1)
  }

  test("IntColumnStats") {
    var c = new ColumnStats.IntColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(65537)
    assert(c.min == -1 && c.max == 65537)
    c.append(-65537)
    assert(c.min == -65537 && c.max == 65537)

    c = new ColumnStats.IntColumnStats
    assert(c.isOrdered && c.isAscending && c.isDescending)
    c.appendNull()
    assert(c.maxDelta == 0)

    c = new ColumnStats.IntColumnStats
    Array(1).foreach(c.append)
    assert(c.isOrdered && c.isAscending && c.isDescending)
    c.appendNull()
    assert(c.maxDelta == 0)

    c = new ColumnStats.IntColumnStats
    Array(1, 2, 3, 3, 4, 22).foreach(c.append)
    assert(c.isOrdered && c.isAscending && !c.isDescending)
    c.appendNull()
    c.appendNull()
    assert(c.maxDelta == 18)

    c = new ColumnStats.IntColumnStats
    Array(22, 1, 0, -5).foreach(c.append)
    assert(c.isOrdered && !c.isAscending && c.isDescending)
    c.appendNull()
    c.appendNull()
    c.appendNull()
    assert(c.maxDelta == 21)

    c = new ColumnStats.IntColumnStats
    Array(22, 1, 24).foreach(c.append)
    c.appendNull()
    assert(!c.isOrdered && !c.isAscending && !c.isDescending)
  }

  test("LongColumnStats") {
    var c = new ColumnStats.LongColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    c.appendNull()
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(Int.MaxValue.toLong + 1L)
    assert(c.min == -1 && c.max == Int.MaxValue.toLong + 1L)
    c.append(Int.MinValue.toLong - 1L)
    assert(c.min == Int.MinValue.toLong - 1L && c.max == Int.MaxValue.toLong + 1L)
    assert(c.nullCount == 1)
  }

  test("FloatColumnStats") {
    var c = new ColumnStats.FloatColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.appendNull()
    c.append(1)
    c.appendNull()
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.appendNull()
    c.append(20.5445F)
    assert(c.min == -1 && c.max == 20.5445F)
    c.append(-20.5445F)
    assert(c.min == -20.5445F && c.max == 20.5445F)
    assert(c.nullCount == 3)
  }

  test("DoubleColumnStats") {
    var c = new ColumnStats.DoubleColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    c.appendNull()
    assert(c.min == -1 && c.max == 1)
    c.append(20.5445)
    assert(c.min == -1 && c.max == 20.5445)
    c.append(-20.5445)
    assert(c.min == -20.5445 && c.max == 20.5445)
    c.appendNull()
    c.appendNull()
    assert(c.nullCount == 3)
  }

  test("TimestampColumnStats") {
    var c = new ColumnStats.TimestampColumnStats
    val ts1 = new Timestamp(1000)
    val ts2 = new Timestamp(2000)
    val ts3 = new Timestamp(1500)
    val ts4 = new Timestamp(2000)
    ts4.setNanos(100)
    c.append(ts1)
    c.appendNull()
    assert(c.min.equals(ts1) && c.max.equals(ts1))
    c.append(ts2)
    assert(c.min.equals(ts1) && c.max.equals(ts2))
    c.append(ts3)
    assert(c.min.equals(ts1) && c.max.equals(ts2))
    c.appendNull()
    c.appendNull()
    assert(c.min.equals(ts1) && c.max.equals(ts2))
    c.append(ts4)
    assert(c.min.equals(ts1) && c.max.equals(ts4))
    assert(c.nullCount == 3)
  }

  test("StringColumnStats") {
    implicit def T(str: String): Text = new Text(str)
    var c = new ColumnStats.StringColumnStats
    assert(c.min == null && c.max == null)
    c.append("a")
    assert(c.min.equals(T("a")) && c.max.equals(T("a")))
    c.appendNull()
    assert(c.min.equals(T("a")) && c.max.equals(T("a")))
    c.append("b")
    assert(c.min.equals(T("a")) && c.max.equals(T("b")))
    c.append("b")
    assert(c.min.equals(T("a")) && c.max.equals(T("b")))
    c.append("cccc")
    c.appendNull()
    assert(c.min.equals(T("a")) && c.max.equals(T("cccc")))
    c.append("0987")
    assert(c.min.equals(T("0987")) && c.max.equals(T("cccc")))
    assert(c.nullCount == 2)
  }
}
