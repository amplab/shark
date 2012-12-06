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

package org.apache.hadoop.hive.ql.exec

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Text

import shark.memstore.ColumnarStructObjectInspector.IDStructField
import shark.memstore.TableStats


object MapSplitPruning {
  // Kept in this package to access package-level visibility variables.

  /**
   * Test whether we should keep the split as a candidate given the filter
   * predicates. Return true if the split should be kept as a candidate, false
   * if the split should be pruned.
   *
   * s and s.stats must not be null here.
   */
  def test(s: TableStats, e: ExprNodeEvaluator): Boolean = e match {
    case e: ExprNodeGenericFuncEvaluator => {
      e.genericUDF match {
        case _: GenericUDFOPAnd => test(s, e.children(0)) && test(s, e.children(1))
        case _: GenericUDFOPOr =>  test(s, e.children(0)) || test(s, e.children(1))
        case udf: GenericUDFBaseCompare =>
          testComparisonPredicate(s, udf, e.children(0), e.children(1))
        case _ => true
      }
    }
    case _ => true
  }

  /**
   * Test whether we should keep the split as a candidate given the comparison
   * predicate. Return true if the split should be kept as a candidate, false if
   * the split should be pruned.
   */
  def testComparisonPredicate(
    s: TableStats,
    udf: GenericUDFBaseCompare,
    left: ExprNodeEvaluator,
    right: ExprNodeEvaluator): Boolean = {

    // Try to get the column evaluator.
    val columnEval: ExprNodeColumnEvaluator =
      if (left.isInstanceOf[ExprNodeColumnEvaluator])
        left.asInstanceOf[ExprNodeColumnEvaluator]
      else if (right.isInstanceOf[ExprNodeColumnEvaluator])
        right.asInstanceOf[ExprNodeColumnEvaluator]
      else null

    // Try to get the constant value.
    val constEval: ExprNodeConstantEvaluator =
      if (left.isInstanceOf[ExprNodeConstantEvaluator])
        left.asInstanceOf[ExprNodeConstantEvaluator]
      else if (right.isInstanceOf[ExprNodeConstantEvaluator])
        right.asInstanceOf[ExprNodeConstantEvaluator]
      else null

    if (columnEval != null && constEval != null) {
      // We can prune the partition only if it is a predicate of form
      //  column op const, where op is <, >, =, <=, >=, !=.
      val field = columnEval.field.asInstanceOf[IDStructField]
      val value: Object = constEval.expr.getValue

      s.stats(field.fieldID) match {
        case Some(columnStats) => {
          val min = columnStats.min
          val max = columnStats.max
          udf match {
            case _: GenericUDFOPEqual => testEqual(min, max, value)
            case _: GenericUDFOPEqualOrGreaterThan => testEqualOrGreaterThan(min, max, value)
            case _: GenericUDFOPEqualOrLessThan => testEqualOrLessThan(min, max, value)
            case _: GenericUDFOPGreaterThan => testGreaterThan(min, max, value)
            case _: GenericUDFOPLessThan => testLessThan(min, max, value)
            case _ => true
          }
        }
        // If there is no stats on the column, don't prune.
        case None => true
      }
    } else {
      // If the predicate is not of type column op value, don't prune.
      true
    }
  }

  def testEqual(min: Any, max: Any, value: Any): Boolean = {
    // Assume min and max have the same type.
    tryCompare(min, value) match {
      case Some(c) => c <= 0 && tryCompare(max, value).get >= 0
      case None => true
    }
  }

  def testEqualOrGreaterThan(min: Any, max: Any, value: Any): Boolean = {
    // Assume min and max have the same type.
    tryCompare(max, value) match {
      case Some(c) => c >= 0
      case None => true
    }
  }

  def testEqualOrLessThan(min: Any, max: Any, value: Any): Boolean = {
    // Assume min and max have the same type.
    tryCompare(min, value) match {
      case Some(c) => c <= 0
      case None => true
    }
  }

  def testGreaterThan(min: Any, max: Any, value: Any): Boolean = {
    // Assume min and max have the same type.
    tryCompare(max, value) match {
      case Some(c) => c > 0
      case None => true
    }
  }

  def testLessThan(min: Any, max: Any, value: Any): Boolean = {
    // Assume min and max have the same type.
    tryCompare(min, value) match {
      case Some(c) => c < 0
      case None => true
    }
  }

  def testNotEqual(min: Any, max: Any, value: Any): Boolean = {
    // Assume min and max have the same type.
    tryCompare(min, value) match {
      case Some(c) => c != 0 || (tryCompare(max, value).get != 0)
      case None => true
    }
  }

  /**
   * Try to compare value a and b.
   * If a is greater than b, return 1.
   * If a equals b, return 0.
   * If a is less than b, return -1.
   * If a and b are not comparable, return None.
   */
  def tryCompare(a: Any, b: Any): Option[Int] = a match {
    case a: Number => b match {
      case b: Number => Some((a.longValue - b.longValue).toInt)
      case _ => None
    }
    case a: Boolean => b match {
      case b: Boolean => Some(if (a && !b) 1 else if (!a && b) -1 else 0)
      case _ => None
    }
    case a: Text => b match {
      case b: Text => Some(a.compareTo(b))
      case b: String => Some(a.compareTo(new Text(b)))
      case _=> None
    }
    case a: String => b match {
      case b: Text => Some((new Text(a)).compareTo(b))
      case b: String => Some(a.compareTo(b))
      case _ => None
    }
    case _ => None
  }
}
