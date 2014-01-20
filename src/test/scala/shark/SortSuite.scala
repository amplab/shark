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

import org.apache.hadoop.io.BytesWritable

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import shark.execution.{ReduceKey, ReduceKeyMapSide, ReduceKeyReduceSide, RDDUtils}


class SortSuite extends FunSuite {

  TestUtils.init()

  var sc: SparkContext = SharkRunner.init()

  test("order by limit") {
    val data = Array((4, 14), (1, 11), (7, 17), (0, 10))
    val expected = data.sortWith(_._1 < _._1).toSeq
    val rdd: RDD[(ReduceKey, BytesWritable)] = sc.parallelize(data, 50).map { x =>
      (new ReduceKeyMapSide(new BytesWritable(Array[Byte](x._1.toByte))),
        new BytesWritable(Array[Byte](x._2.toByte)))
    }
    for (k <- 0 to 5) {
      val sortedRdd = RDDUtils.topK(rdd, k).asInstanceOf[RDD[(ReduceKeyReduceSide, Array[Byte])]]
      val output = sortedRdd.map { case(k, v) =>
        (k.byteArray(0).toInt, v(0).toInt)
      }.collect().toSeq
      assert(output.size === math.min(k, 4))
      assert(output === expected.take(math.min(k, 4)))
    }
  }

}
