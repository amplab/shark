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

import org.apache.hadoop.hive.ql.plan.JoinCondDesc
import org.scalatest.{BeforeAndAfter, FunSuite}
import shark.execution.ReduceKey
import spark.{RDD, SparkContext}
import spark.SparkContext._


class SortSuite extends FunSuite {

  TestUtils.init()

  test("order by limit") {
    val sc = new SparkContext("local", "test")
    val data = Array((new ReduceKey(Array[Byte](4)), "val_4"),
                     (new ReduceKey(Array[Byte](1)), "val_1"),
                     (new ReduceKey(Array[Byte](7)), "val_7"),
                     (new ReduceKey(Array[Byte](0)), "val_0"))
    val expected = data.sortWith(_._1 < _._1).toSeq
    val rdd = sc.parallelize(data, 50)
    for (k <- 0 to 5) {
      val output = RDDUtils.sortLeastKByKey(rdd, k).collect().toSeq
      assert(output.size == math.min(k, 4))
      assert(output == expected.take(math.min(k, 4)))
    }
    sc.stop()
  }
}
