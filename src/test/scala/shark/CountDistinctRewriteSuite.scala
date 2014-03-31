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

import scala.collection.JavaConversions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}
import org.apache.hadoop.hive.ql.parse.{ParseDriver, ParseUtils}

import shark.api.QueryExecutionException
import shark.parse.ASTRewriteUtil
import shark.parse.ASTRewriteUtil._


class CountDistinctRewriteSuite extends FunSuite with BeforeAndAfterAll {

  /**
   * Throws an error if this is not equal to other.
   *
   * Right now this function only checks the name, type, text and children of the node
   * for equality.
   */
  def checkEquals(node: ASTNode, other: ASTNode) {
    def check(field: String, func: ASTNode => Any) {
      assert(func(node) == func(other),
        "node: \n" + printTree(node) + "\nis not equal to: \n" + printTree(other) +
        "\n for field: " + field)
    }

    check("name", _.getName)
    check("type", _.getType)
    check("text", _.getText)
    check("numChildren", (node: ASTNode) => getChildren(node).size)

    val leftChildren = getChildren(node)
    val rightChildren = getChildren(other)
    leftChildren.zip(rightChildren).foreach {
      case (l,r) => checkEquals(l, r)
    }
  }

  def genAST(command: String): ASTNode = {
    try {
      ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command))
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to parse: " + command)
    }
  }

  test("Count distinct, single column") {
    val command = genAST("select count(distinct key) from src")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("select count(*) from (select distinct key from src) %s"
      .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("Count distinct, multiple columns") {
    val command = genAST("select count(distinct key, value) from src")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("select count(*) from (select distinct key, value from src) %s"
      .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("Multiple columns with expressions") {
    val command = genAST("select count(distinct key * 10 - 3, substr(value, 5)) from src")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST(
      "select count(*) from (select distinct key * 10 - 3, substr(value, 5) from src) %s"
      .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("Distinct function outputs") {
    val command = genAST("select count(distinct substr(val, 5)) from src")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select count(*) from (select distinct substr(val, 5) from src) %s"""
        .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("Constants aside COUNT DISTINCT in SELECT list") {
    val command1 = genAST("select 1, 2, count(distinct key) from src")
    val rewrite1 = ASTRewriteUtil.countDistinctToGroupBy(command1)
    val expectedRewrite1 = genAST("""
      select 1, 2, count(*) from (select distinct key from src) %s"""
        .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite1, expectedRewrite1)

    val command2 = genAST("select 1, count(distinct key), 2, 3 from src")
    val rewrite2 = ASTRewriteUtil.countDistinctToGroupBy(command2)
    val expectedRewrite2 = genAST("""
      select 1, count(*), 2, 3 from (select distinct key from src) %s"""
        .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite2, expectedRewrite2)
  }

  test("COUNT DISTINCT as part of an expression") {
    val command = genAST("select count(distinct key) + 10 from src")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select count(*) + 10 from (select distinct key from src) %s"""
        .format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("COUNT DISTINCT as part of a subquery") {
    val command = genAST("select * from (select count(distinct key) + 10 from src) numDistincts")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select * from
        (select count(*) + 10 from
          (select distinct key from src) %s) numDistincts
      """.format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "1"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("COUNT DISTINCT from results of a subquery") {
    val command = genAST("""
      select count(distinct a.val) from
        (select * from src where key is null) a
        join
        (select * from src where key is null) b on a.key = b.key
      """)
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select count(*) from
        (select distinct a.val from
          (select * from src where key is null) a
          join
          (select * from src where key is null) b on a.key = b.key
        ) %s
      """.format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "0"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("COUNT DISTINCT from the results of a subquery, as part of an outer subquery") {
    val command = genAST("""
      select * from (
        select count(distinct a.val) from
          (select * from src where key is null) a
          join
          (select * from src where key is null) b on a.key = b.key
        ) numDistincts
      """)
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select * from
        (select count(*) from
          (select distinct a.val from
            (select * from src where key is null) a
            join
            (select * from src where key is null) b on a.key = b.key
          ) %s
        ) numDistincts 
      """.format(ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "1"))
    checkEquals(rewrite, expectedRewrite)
  }

  test("Union multiple count distincts") {
    val command = genAST("""
      select * from (
        select count(distinct key) from src
        union all
        select count(distinct value) from src
        union all
        select count(distinct key) from src1
        union all
        select count(distinct value) from src2
      ) distinctKVs""")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select * from (
        select count(*) from (select distinct key from src) %s
        union all
        select count(*) from (select distinct value from src) %s
        union all
        select count(*) from (select distinct key from src1) %s
        union all
        select count(*) from (select distinct value from src2) %s
      ) distinctKVs""".format(
        ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "3",
        ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "4",
        ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "2",
        ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "1"))
    checkEquals(rewrite, expectedRewrite)    
  }

  test("Union multiple count distincts, both over subqueries") {
    val command = genAST("""
      select * from (
        select count(distinct a.key) from
          (select * from src where key is null) a
          join
          (select * from src where key is null) b on a.key = b.key
        union all
        select count(distinct c.value) from
          (select * from src where value is null) c
          join
          (select * from src where value is null) d on c.value = d.value
        ) distinctKVs""")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    val expectedRewrite = genAST("""
      select * from (
        select count(*) from
          (select distinct a.key from
            (select * from src where key is null) a
            join
            (select * from src where key is null) b on a.key = b.key
          ) %s
        union all
        select count(*) from
          (select distinct c.value from
            (select * from src where value is null) c
            join
            (select * from src where value is null) d on c.value = d.value
          ) %s 
        ) distinctKVs""".format(
          ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "1",
          ASTRewriteUtil.DISTINCT_SUBQUERY_ALIAS + "2"))
    checkEquals(rewrite, expectedRewrite)    
  }

  test("Multiple COUNT DISTINCT in SELECT expression list isn't rewritten (or supported yet)") {
    val command = genAST("""
      select
        sum(key),
        count(distinct key),
        count(distinct value)
      from src""")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    checkEquals(command, rewrite)
  }

  test("COUNT DISTINCT with partitioning key isn't rewritten") {
    val command = genAST("select key, count(distinct value) from src group by key")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    checkEquals(command, rewrite)
  }

  test("COUNT DISTINCT with LIMIT isn't rewritten") {
    val command = genAST("select key, count(distinct value) from src limit 10")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    checkEquals(command, rewrite)
  }

  test("COUNT DISTINCT with CUBE and GROUP BY isn't rewritten") {
    val command = genAST("select key, count(distinct value) from src group by key with cube")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    checkEquals(command, rewrite)
  }

  test("COUNT DISTINCT with ROLLUP and GROUP BY isn't rewritten") {
    val command = genAST("select key, count(distinct value) from src group by key with rollup")
    val rewrite = ASTRewriteUtil.countDistinctToGroupBy(command)
    checkEquals(command, rewrite)
  }

}
