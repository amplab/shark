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

package shark

import java.sql.Timestamp

import org.apache.hadoop.hive.ql.exec.MapSplitPruning._
import org.apache.hadoop.io.Text

import org.scalatest.FunSuite


class MapSplitPruningSuite extends FunSuite {

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

    assert(testEqual(new Timestamp(100), new Timestamp(200), new Timestamp(100)) == true)
    assert(testEqual(new Timestamp(100), new Timestamp(200), new Timestamp(101)) == true)
    assert(testEqual(new Timestamp(100), new Timestamp(200), new Timestamp(200)) == true)
    assert(testEqual(new Timestamp(100), new Timestamp(200), new Timestamp(99)) == false)
    assert(testEqual(new Timestamp(100), new Timestamp(200), new Timestamp(201)) == false)

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

    assert(testGreaterThan(new Timestamp(100), new Timestamp(200), new Timestamp(100)) == true)
    assert(testGreaterThan(new Timestamp(100), new Timestamp(200), new Timestamp(101)) == true)
    assert(testGreaterThan(new Timestamp(100), new Timestamp(200), new Timestamp(200)) == false)
    assert(testGreaterThan(new Timestamp(100), new Timestamp(200), new Timestamp(99)) == true)
    assert(testGreaterThan(new Timestamp(100), new Timestamp(200), new Timestamp(201)) == false)

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

    assert(testEqualOrGreaterThan(
        new Timestamp(100), new Timestamp(200), new Timestamp(100)) == true)
    assert(testEqualOrGreaterThan(
        new Timestamp(100), new Timestamp(200), new Timestamp(101)) == true)
    assert(testEqualOrGreaterThan(
        new Timestamp(100), new Timestamp(200), new Timestamp(200)) == true)
    assert(testEqualOrGreaterThan(
        new Timestamp(100), new Timestamp(200), new Timestamp(99)) == true)
    assert(testEqualOrGreaterThan(
        new Timestamp(100), new Timestamp(200), new Timestamp(201)) == false)

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

    assert(testLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(100)) == false)
    assert(testLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(101)) == true)
    assert(testLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(200)) == true)
    assert(testLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(99)) == false)
    assert(testLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(201)) == true)

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

    assert(testEqualOrLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(100)) == true)
    assert(testEqualOrLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(101)) == true)
    assert(testEqualOrLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(200)) == true)
    assert(testEqualOrLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(99)) == false)
    assert(testEqualOrLessThan(new Timestamp(100), new Timestamp(200), new Timestamp(201)) == true)

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

    assert(testNotEqual(new Timestamp(100), new Timestamp(200), new Timestamp(100)) == true)
    assert(testNotEqual(new Timestamp(100), new Timestamp(200), new Timestamp(101)) == true)
    assert(testNotEqual(new Timestamp(100), new Timestamp(200), new Timestamp(200)) == true)
    assert(testNotEqual(new Timestamp(100), new Timestamp(200), new Timestamp(99)) == true)
    assert(testNotEqual(new Timestamp(100), new Timestamp(200), new Timestamp(201)) == true)
    assert(testNotEqual(new Timestamp(100), new Timestamp(100), new Timestamp(100)) == false)

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
