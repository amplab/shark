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

import java.util.{List => JavaList}

import org.apache.hadoop.hive.ql.optimizer.JoinReorder
import org.apache.hadoop.hive.ql.optimizer.{Optimizer => HiveOptimizer, 
  SimpleFetchOptimizer, Transform}
import org.apache.hadoop.hive.ql.parse.{ParseContext}

class SharkOptimizer extends HiveOptimizer with LogHelper {

  /**
   * Override Hive optimizer to skip SimpleFetchOptimizer, which is designed 
   * to let Hive avoid launching MR jobs on simple queries, but rewrites the 
   * query plan in a way that is inconvenient for Shark (replaces the FS operator 
   * with a non-terminal ListSink operator).
   */
  override def optimize(): ParseContext  = {

    // Use reflection to make some private members accessible.
    val transformationsField = classOf[HiveOptimizer].getDeclaredField("transformations")
    val pctxField = classOf[HiveOptimizer].getDeclaredField("pctx")
    pctxField.setAccessible(true)
    transformationsField.setAccessible(true)
    val transformations = transformationsField.get(this).asInstanceOf[JavaList[Transform]]
    var pctx = pctxField.get(this).asInstanceOf[ParseContext]

    // Invoke each optimizer transformation
    val it = transformations.iterator
    while (it.hasNext()) {
      val transformation = it.next()
      transformation match {
        case _: SimpleFetchOptimizer => {}
        case _: JoinReorder => {}
        case _ => {
          pctx = transformation.transform(pctx)
        }
      }
    }
    pctx
  }
}
