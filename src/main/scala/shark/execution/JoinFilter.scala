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

import java.util.{HashMap => JHashMap, List => JList}

import scala.reflect.BeanProperty
import scala.collection.mutable.Queue

import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.BooleanWritable


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
  protected def setColumnValues(data: AnyRef, outputRow: Array[AnyRef], tblIdx: Int,
    offsets: IndexedSeq[Int]) {
    
    val columns = data.asInstanceOf[Array[AnyRef]]
    val size = joinVals(tblIdx).size()
    
    System.arraycopy(columns, 0, outputRow, offsets(tblIdx), size)
  }
  
    /**
     * Create the new output tuple(s), and put the outputs into the outputRows. The join sequence
     * generally looks like:
     * 
     *                        join (filter value eval)  ...
     *                      /                           \
     *             join (filter value eval)              \
     *          /                    \                    \
     * table1(col1,col2..)   table2(colx, coly..)   table3(cola, colb..)   ...
     * 
     * This function iterates input tables one by one sequentially (from left to right), 
     * more precisely 
     * 1) "Full Outer Join" may outputs 1 or more rows
     * 2) "Inner Join" may outputs 1 or less rows
     * 3) "Left/Right Outer Join" only outputs single row.
     * 4) "Left Semi Join" works like the "Left Outer Join", but only the first row outputs, usually
     *    it used in checking the "existence of relationship"
     * @Param elements [input] represents values of all input tables columns 
     * @Param previousRightFiltered [input] represents the left columns should be discarded or not
     *                              true for discarding the columns
     * @Param previousRightTable [input] represents left columns value
     * @Param startIdx [input] left table index in the join sequence
     * @Return this is for left semi join, false to indicate no more record needed. otherwise true
     * 
     * TODO:Rows from either side that do not match the join conditions should be included 
     * only once in the output (with the other sides values set to NULL). This implementation 
     * leads to over inclusion(by marmbrus). But currently, let's keep the same behavior as Hive.
     */
    def generate[B <: AnyRef](elements: Array[B]): Array[AnyRef] = {
      import java.util.Arrays
      
      Arrays.fill(rowBuffer, 0, resultRowSize, null)
      
      var previousRightFiltered: Boolean = false
      var previousRightTable: AnyRef = null
      
      var index = 0

      var leftTable = if(index == 0) {
        elements(0)
      } else {
        previousRightTable
      }
      
      var leftFiltered = if(index == 0) {
        filterEval(leftTable)
      } else {
        previousRightFiltered
      }

      var entireLeftFiltered = leftFiltered
      
      while (index < joinConditions.size) {
        val joinCondition = joinConditions(index)
        val rightTableIndex  = index + 1
        
        var rightTable = elements(rightTableIndex)

        var rightFiltered = filterEval(rightTable)

        joinCondition.getType() match {
          case CommonJoinOperator.FULL_OUTER_JOIN => {
            //
            // CartesianProduct already contains all of the possible input entries
            // Do nothing here.
            //
            rightFiltered = false
            leftFiltered = false
          }
          case CommonJoinOperator.LEFT_OUTER_JOIN => {
            if (entireLeftFiltered || rightFiltered) {
              // will not output anything for the right table columns
              rightFiltered = true
            }
            leftFiltered = false
          }
          case CommonJoinOperator.RIGHT_OUTER_JOIN => {
            // setColumnValues(rightTable, row, rightTableIndex.toByte, offsets)
            
            if (entireLeftFiltered || rightFiltered) {
              // if filtered then reset all of the left tables columns
              Arrays.fill(rowBuffer, 0, inputOffsets(rightTableIndex), null)
              leftFiltered = true
            }
            rightFiltered = false
          }
          case CommonJoinOperator.LEFT_SEMI_JOIN => {
            // the same with left outer join
            if (entireLeftFiltered || rightFiltered) {
              // will not output anything for the right table columns
              rightFiltered = true
            } else {
              // find the valid output, and will not output new row any more
            }
            leftFiltered = false
          }
          case CommonJoinOperator.INNER_JOIN => {
            if (entireLeftFiltered || rightFiltered) {
              Arrays.fill(rowBuffer, 0, inputOffsets(joinCondition.getRight()), null)
              leftFiltered  = true
              rightFiltered = true
            }
          }
          case _ => assert(false)
        }
        
        leftFiltered = leftFiltered || (leftTable == null)
        rightFiltered = rightFiltered || (rightTable == null)
        
        // set the left table columns
        if(!leftFiltered) {
          setColumnValues(leftTable, rowBuffer, index, inputOffsets)
        }

        entireLeftFiltered = entireLeftFiltered && leftFiltered && rightFiltered
        
        leftFiltered = rightFiltered
        leftTable = rightTable
        index = rightTableIndex
      }
      
      // set the most right table columns
      if(!leftFiltered) {
        setColumnValues(leftTable, rowBuffer, index, inputOffsets)
      }
      
      rowBuffer
    }
}
