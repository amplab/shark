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

import java.util.{Arrays => JArrays}

import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.plan.JoinDesc


trait JoinFilter[T <: JoinDesc] {
  self: CommonJoinOperator[T] =>
    
  @BeanProperty var resultTupleSizes: Array[Int] = _
  @BeanProperty var inputOffsets: Array[Int] = _
  @BeanProperty var resultRowSize: Int = 0 
  
  protected def initializeJoinFilterOnMaster() {
    // the columns count of the input tables in the final output tuple
    resultTupleSizes = (0 until joinVals.size).map {i => joinVals(i).size()}.toArray[Int]
    inputOffsets = resultTupleSizes.scanLeft(0)(_ + _)
    resultRowSize = inputOffsets.last
  }

  /**
   * Copy the table(input) columns value to the output tuple
   */
  protected def setColumnValues(
      data: AnyRef,
      outputRow: Array[AnyRef],
      tblIdx: Int,
      offsets: IndexedSeq[Int])
  {
    val columns = data.asInstanceOf[Array[AnyRef]]
    val size = joinVals(tblIdx).size()
    System.arraycopy(columns, 0, outputRow, offsets(tblIdx), size)
  }
  
  /**
   * Create the new output tuple based on the input elements and return it.
   *
   * The join sequence generally looks like:
   *
   *                        join (filter value eval)  ...
   *                      /                           \
   *             join (filter value eval)              \
   *          /                    \                    \
   * table1(col1,col2..)   table2(colx, coly..)   table3(cola, colb..)   ...
   *
   * @param elements [input] represents values of all input tables columns
   */
  def generate[B <: AnyRef](elements: Array[B]): Array[AnyRef] = {
    JArrays.fill(rowBuffer, 0, resultRowSize, null)
    var index = 0
    var leftTable = elements(0)
    var leftFiltered = filterEval(leftTable)

    var entireLeftFiltered = leftFiltered

    while (index < joinConditions.size) {
      val joinCondition = joinConditions(index)
      val rightTableIndex  = index + 1

      val rightTable = elements(rightTableIndex)

      var rightFiltered = filterEval(rightTable)

      joinCondition.getType match {
        case CommonJoinOperator.FULL_OUTER_JOIN =>
          // CartesianProduct already contains all of the possible input entries
          // Do nothing here.
          rightFiltered = false
          leftFiltered = false

        case CommonJoinOperator.LEFT_OUTER_JOIN =>
          if (entireLeftFiltered || rightFiltered) {
            // will not output anything for the right table columns
            rightFiltered = true
          }
          leftFiltered = false

        case CommonJoinOperator.RIGHT_OUTER_JOIN =>
          // setColumnValues(rightTable, row, rightTableIndex.toByte, offsets)

          if (entireLeftFiltered || rightFiltered) {
            // if filtered then reset all of the left tables columns
            JArrays.fill(rowBuffer, 0, inputOffsets(rightTableIndex), null)
            leftFiltered = true
          }
          rightFiltered = false

        case CommonJoinOperator.LEFT_SEMI_JOIN =>
          // the same with left outer join
          if (entireLeftFiltered || rightFiltered) {
            // will not output anything for the right table columns
            rightFiltered = true
          } else {
            // find the valid output, and will not output new row any more
          }
          leftFiltered = false

        case CommonJoinOperator.INNER_JOIN =>
          if (entireLeftFiltered || rightFiltered) {
            JArrays.fill(rowBuffer, 0, inputOffsets(joinCondition.getRight()), null)
            leftFiltered  = true
            rightFiltered = true
          }

        case _ =>
          throw new UnsupportedOperationException("Unsupported join type " + joinCondition.getType)
      }

      leftFiltered = leftFiltered || (leftTable == null)
      rightFiltered = rightFiltered || (rightTable == null)

      // set the left table columns
      if (!leftFiltered) {
        setColumnValues(leftTable, rowBuffer, index, inputOffsets)
      }

      entireLeftFiltered = entireLeftFiltered && leftFiltered && rightFiltered

      leftFiltered = rightFiltered
      leftTable = rightTable
      index = rightTableIndex
    }

    // set the most right table columns
    if (!leftFiltered) {
      setColumnValues(leftTable, rowBuffer, index, inputOffsets)
    }

    rowBuffer
  }
}
