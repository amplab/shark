package shark

import org.apache.hadoop.hive.ql.plan.JoinCondDesc
import org.scalatest.{BeforeAndAfter, FunSuite}
import shark.execution.ReduceKey
import spark.{RDD, SparkContext}
import spark.SparkContext._


class RDDSuite extends FunSuite {

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
  }
}
