package shark

import org.apache.hadoop.hive.ql.plan.JoinCondDesc
import org.scalatest.{BeforeAndAfter, FunSuite}
import shark.exec.CommonJoinOperator
import spark.{RDD, SparkContext}
import spark.SparkContext._

/*
class JoinSuite extends FunSuite {
  
  test("basic pair join") {
    val sc = new SparkContext("local", "test")
    val rdds = Seq(
      sc.parallelize(Array((1, "1"), (2, "2"), (3, "3"), (4, "4"))),
      sc.parallelize(Array((1, "11"), (2, "22"), (3, "33"), (5, "5")))
    ).asInstanceOf[Seq[RDD[(Any, Any)]]]
    
    // inner join
    var result = CommonJoinOperator.join(
      rdds,
      Array(new JoinCondDesc(0, 1, CommonJoinOperator.INNER_JOIN)),
      2).collect()
    var expected = Seq(
      (Seq("1", "11")), (Seq("2", "22")), (Seq("3", "33"))
    )
    expected.foreach { row =>
      assert(result.contains(row),
        "inner join result does not contain row: " + row + "\n" + result.toSeq)
    }
    assert(result.size === expected.size)
    
    // full outer join
    result = CommonJoinOperator.join(
      rdds,
      Array(new JoinCondDesc(0, 1, CommonJoinOperator.FULL_OUTER_JOIN)),
      2).collect()
    expected = Seq(
      (Seq("1", "11")), (Seq("2", "22")), (Seq("3", "33")), (Seq("4", null)), (Seq(null, "5"))
    )
    expected.foreach { row =>
      assert(result.contains(row),
        "full outer result does not contain row: " + row + "\n" + result.toSeq)
    }
    assert(result.size === expected.size)

    // left outer join
    result = CommonJoinOperator.join(
      rdds,
      Array(new JoinCondDesc(0, 1, CommonJoinOperator.LEFT_OUTER_JOIN)),
      2).collect()
    expected = Seq(
      (Seq("1", "11")), (Seq("2", "22")), (Seq("3", "33")), (Seq("4", null))
    )
    expected.foreach { row =>
      assert(result.contains(row),
        "left outer result does not contain row: " + row + "\n" + result.toSeq)
    }
    assert(result.size === expected.size)

    // right outer join
    result = CommonJoinOperator.join(
      rdds,
      Array(new JoinCondDesc(0, 1, CommonJoinOperator.RIGHT_OUTER_JOIN)),
      2).collect()
    expected = Seq(
      (Seq("1", "11")), (Seq("2", "22")), (Seq("3", "33")), (Seq(null, "5"))
    )
    expected.foreach { row =>
      assert(result.contains(row),
        "right outer result does not contain row: " + row + "\n" + result.toSeq)
    }
    assert(result.size === expected.size)


    sc.stop()
  }

  test("three way inner join") {
    val sc = new SparkContext("local", "test")
    val rdds = Seq(
      sc.parallelize(Array((1, "1"), (2, "2-1"), (2, "2-2"), (3, "3"), (4, "4"))),
      sc.parallelize(Array((1, "11"), (2, "22-1"), (2, "22-2"), (3, "33"))),
      sc.parallelize(Array((1, "111"), (2, "222-1"), (3, "333")))
    ).asInstanceOf[Seq[RDD[(Any, Any)]]]

    val expected = Seq(
      (Seq("1", "11", "111")),
      (Seq("2-1", "22-1", "222-1")),
      (Seq("2-1", "22-2", "222-1")),
      (Seq("2-2", "22-1", "222-1")),
      (Seq("2-2", "22-2", "222-1")),
      (Seq("3", "33", "333"))
    )

    val joinConditions = Array(
      new JoinCondDesc(0, 1, CommonJoinOperator.INNER_JOIN),
      new JoinCondDesc(0, 2, CommonJoinOperator.INNER_JOIN))
    var result = CommonJoinOperator.join(rdds, joinConditions, 2).collect()
    expected.foreach { row =>
      assert(result.contains(row),
        "result does not contain row: " + row + "\n" + result.toSeq)
    }
    assert(result.size === expected.size)

    sc.stop()
  }

  test("three way join") {
    val sc = new SparkContext("local", "test")
    val rdds = Seq(
      sc.parallelize(Array((1, "1"), (2, "2-1"), (2, "2-2"), (3, "3"), (4, "4"))),
      sc.parallelize(Array((1, "11"), (2, "22-1"), (2, "22-2"), (3, "33"))),
      sc.parallelize(Array((1, "111"), (2, "222-1"), (3, "333"), (5, "555")))
    ).asInstanceOf[Seq[RDD[(Any, Any)]]]

    val expected = Seq(
      (Seq("1", "11", "111")),
      (Seq("2-1", "22-1", "222-1")),
      (Seq("2-1", "22-2", "222-1")),
      (Seq("2-2", "22-1", "222-1")),
      (Seq("2-2", "22-2", "222-1")),
      (Seq("3", "33", "333")),
      (Seq("4", null, null)),
      (Seq(null, null, "555"))
    )

    val joinConditions = Array(
      new JoinCondDesc(0, 1, CommonJoinOperator.LEFT_OUTER_JOIN),
      new JoinCondDesc(0, 2, CommonJoinOperator.FULL_OUTER_JOIN))
    var result = CommonJoinOperator.join(rdds, joinConditions, 2).collect()
    expected.foreach { row =>
      assert(result.contains(row),
        "result does not contain row: " + row + "\n" + result.toSeq)
    }
    assert(result.size === expected.size)

    sc.stop()
  }

}
*/
