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

import org.apache.hadoop.hive.serde2.objectinspector.{MapSplitPruningHelper, StructField}
import org.apache.hadoop.hive.serde2.objectinspector.UnionStructObjectInspector.MyField
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr

import shark.memstore2.ColumnarStructObjectInspector.IDStructField
import shark.memstore2.TablePartitionStats


object MapSplitPruning {
  // Kept in this package to access package-level visibility variables.

  /**
   * Test whether we should keep the split as a candidate given the filter
   * predicates. Return true if the split should be kept as a candidate, false
   * if the split should be pruned.
   *
   * s and s.stats must not be null here.
   */
  def test(s: TablePartitionStats, e: ExprNodeEvaluator): Boolean = {
    if (s.numRows == 0) {
      // If the partition is empty, it can be pruned.
      false
    } else {
      // If the partition is not empty, test the condition based on the operator.
      e match {
        case e: ExprNodeGenericFuncEvaluator => {
          e.genericUDF match {
            case _: GenericUDFOPAnd => test(s, e.children(0)) && test(s, e.children(1))
            case _: GenericUDFOPOr => test(s, e.children(0)) || test(s, e.children(1))
            case _: GenericUDFBetween =>
              val col = e.children(1)
              if (col.isInstanceOf[ExprNodeColumnEvaluator]) {
                testBetweenPredicate(s, e.children(0).asInstanceOf[ExprNodeConstantEvaluator],
                  col.asInstanceOf[ExprNodeColumnEvaluator],
                  e.children(2).asInstanceOf[ExprNodeConstantEvaluator],
                  e.children(3).asInstanceOf[ExprNodeConstantEvaluator])
              } else {
                //cannot prune function based evaluators in general.
                true
              }

            case _: GenericUDFIn =>
              testInPredicate(
                s,
                e.children(0).asInstanceOf[ExprNodeColumnEvaluator],
                e.children.drop(1))
            case udf: GenericUDFBaseCompare =>
              testComparisonPredicate(s, udf, e.children(0), e.children(1))
            case _ => true
          }
        }
        case _ => true
      }
    }
  }

  def testInPredicate(
    s: TablePartitionStats,
    columnEval: ExprNodeColumnEvaluator,
    expEvals: Array[ExprNodeEvaluator]): Boolean = {

    val field = getIDStructField(columnEval.field)
    val columnStats = s.stats(field.fieldID)

    if (columnStats != null) {
      expEvals.exists {
        e =>
          val constEval = e.asInstanceOf[ExprNodeConstantEvaluator]
          columnStats := constEval.expr.getValue()
      }
    } else {
      // If there is no stats on the column, don't prune.
      true
    }
  }

  def testBetweenPredicate(
    s: TablePartitionStats,
    invertEval: ExprNodeConstantEvaluator,
    columnEval: ExprNodeColumnEvaluator,
    leftEval: ExprNodeConstantEvaluator,
    rightEval: ExprNodeConstantEvaluator): Boolean = {

    val field = getIDStructField(columnEval.field)
    val columnStats = s.stats(field.fieldID)
    val leftValue: Object = leftEval.expr.getValue
    val rightValue: Object = rightEval.expr.getValue
    val invertValue: Boolean = invertEval.expr.getValue.asInstanceOf[Boolean]

    if (columnStats != null) {
      val exists = (columnStats :>< (leftValue , rightValue))
      if (invertValue) !exists else exists
    } else {
      // If there is no stats on the column, don't prune.
      true
    }
  }

  /**
   * Test whether we should keep the split as a candidate given the comparison
   * predicate. Return true if the split should be kept as a candidate, false if
   * the split should be pruned.
   */
  def testComparisonPredicate(
    s: TablePartitionStats,
    udf: GenericUDFBaseCompare,
    left: ExprNodeEvaluator,
    right: ExprNodeEvaluator): Boolean = {

    // Try to get the column evaluator.
    val columnEval: ExprNodeColumnEvaluator =
      if (left.isInstanceOf[ExprNodeColumnEvaluator]) {
        left.asInstanceOf[ExprNodeColumnEvaluator]
      } else if (right.isInstanceOf[ExprNodeColumnEvaluator]) {
        right.asInstanceOf[ExprNodeColumnEvaluator]
      } else {
        null
      }

    // Try to get the constant value.
    val constEval: ExprNodeConstantEvaluator =
      if (left.isInstanceOf[ExprNodeConstantEvaluator]) {
        left.asInstanceOf[ExprNodeConstantEvaluator]
      } else if (right.isInstanceOf[ExprNodeConstantEvaluator]) {
        right.asInstanceOf[ExprNodeConstantEvaluator]
      } else {
        null
      }

    if (columnEval != null && constEval != null) {
      // We can prune the partition only if it is a predicate of form
      //  column op const, where op is <, >, =, <=, >=, !=.
      val field = getIDStructField(columnEval.field)
      val value: Object = constEval.expr.getValue
      val columnStats = s.stats(field.fieldID)

      if (columnStats != null) {
        udf match {
          case _: GenericUDFOPEqual => columnStats := value
          case _: GenericUDFOPEqualOrGreaterThan => columnStats :>= value
          case _: GenericUDFOPEqualOrLessThan => columnStats :<= value
          case _: GenericUDFOPGreaterThan => columnStats :> value
          case _: GenericUDFOPLessThan => columnStats :< value
          case _ => true
        }
      } else {
        // If there is no stats on the column, don't prune.
        true
      }
    } else {
      // If the predicate is not of type column op value, don't prune.
      true
    }
  }

  private def getIDStructField(field: StructField): IDStructField = field match {
    case myField: MyField => {
      // For partitioned tables, the ColumnarStruct's IDStructFields are enclosed inside
      // the Hive UnionStructObjectInspector's MyField objects.
      MapSplitPruningHelper.getStructFieldFromUnionOIField(myField)
        .asInstanceOf[IDStructField]
    }
    case idStructField: IDStructField => idStructField
    case otherFieldType: Any => {
      throw new Exception("Unrecognized StructField: " + otherFieldType)
    }
  }
}
