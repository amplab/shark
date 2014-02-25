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

package shark.execution

import java.util.{List => JavaList}

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils => OIUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.{ObjectInspectorCopyOption => CopyOption}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector

import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable

import shark.execution.serialization.SerializableWritable


object JoinUtil {

  def computeJoinKey(row: Any, keyFields: JavaList[ExprNodeEvaluator], keyFieldsOI: JavaList[OI])
  : Seq[SerializableWritable[_]] = {
    Range(0, keyFields.size).map { i =>
      val c = copy(row, keyFields.get(i), keyFieldsOI.get(i), CopyOption.WRITABLE)
      val s = if (c == null) NullWritable.get else c
      new SerializableWritable(s.asInstanceOf[Writable])
    }
  }

  def joinKeyHasAnyNulls(joinKey: Seq[AnyRef], nullSafes: Array[Boolean]): Boolean = {
    joinKey.zipWithIndex.exists { x =>
      (nullSafes == null || nullSafes(x._2).unary_!) && (x._1 == null)
    }
  }

  def computeJoinValues(row: Any,
      valueFields: JavaList[ExprNodeEvaluator],
      valueFieldsOI: JavaList[OI],
      filters: JavaList[ExprNodeEvaluator],
      filtersOI: JavaList[OI],
      noOuterJoin: Boolean,
      serializable: Boolean = false)
    : Array[AnyRef] = {

    // isFiltered = true means failed in the join filter testing
    val isFiltered: Boolean = {
      if (filters == null) {
        false
      } else {
        var x = 0
        var exists = false
        while (x < filters.size() && !exists) {
          val cond = filters.get(x).evaluate(row)
          if (cond == null) {
            exists = true
          } else {
            exists = !filtersOI.get(x).asInstanceOf[BooleanObjectInspector].get(cond)
          }
          x += 1
        }
        
        exists
      }
    }
    val size = valueFields.size
    val a = new Array[AnyRef](size)
    var i = 0
    while (i < size) {
      a(i) = copy(row, valueFields.get(i), valueFieldsOI.get(i), CopyOption.WRITABLE)
      i += 1
    }

    val result = if (noOuterJoin) {
      a
    } else {
      val n = new Array[AnyRef](size + 1)
      Array.copy(a, 0, n, 0, size)
      n(size) = new BooleanWritable(isFiltered)
      n
    }
    
    if (serializable) {
      result.map(e => new SerializableWritable(e.asInstanceOf[Writable]))
    } else {
      result
    }
  }

  private def copy(row: Any, evaluator: ExprNodeEvaluator, oi: OI, copyOption: CopyOption) = {
    OIUtils.copyToStandardObject(evaluator.evaluate(row), oi, copyOption)
  }
}
