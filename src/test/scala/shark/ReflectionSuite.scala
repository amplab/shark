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

import org.scalatest.FunSuite


/**
 * A suite of test to ensure reflections are used properly in Shark to invoke
 * Hive non-public methods. This is needed because we cannot detect reflection
 * errors until runtime. Every time reflection is used to expand visibility of
 * methods or variables, a test should be added.
 */
class ReflectionSuite extends FunSuite {

  test("CliDriver") {
    val c = classOf[org.apache.hadoop.hive.cli.CliDriver]
    var m = c.getDeclaredMethod("getFormattedDb",
      classOf[org.apache.hadoop.hive.conf.HiveConf],
      classOf[org.apache.hadoop.hive.cli.CliSessionState])
    m.setAccessible(true)
    assert(m.getReturnType == classOf[String])

    m = c.getDeclaredMethod(
      "spacesForString", classOf[String])
    m.setAccessible(true)
    assert(m.getReturnType == classOf[String])
  }

  test("Driver") {
    val c = classOf[org.apache.hadoop.hive.ql.Driver]

    var m = c.getDeclaredMethod(
      "doAuthorization", classOf[org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer])
    m.setAccessible(true)
    assert(m.getReturnType === Void.TYPE)

    m = c.getDeclaredMethod("getHooks",
      classOf[org.apache.hadoop.hive.conf.HiveConf.ConfVars], classOf[Class[_]])
    m.setAccessible(true)
    assert(m.getReturnType === classOf[java.util.List[_]])

    var f = c.getDeclaredField("plan")
    f.setAccessible(true)
    assert(f.getType === classOf[org.apache.hadoop.hive.ql.QueryPlan])

    f = c.getDeclaredField("ctx")
    f.setAccessible(true)
    assert(f.getType === classOf[org.apache.hadoop.hive.ql.Context])

    f = c.getDeclaredField("schema")
    f.setAccessible(true)
    assert(f.getType === classOf[org.apache.hadoop.hive.metastore.api.Schema])

    f = c.getDeclaredField("LOG")
    f.setAccessible(true)
    assert(f.getType === classOf[org.apache.commons.logging.Log])
  }

  test("SemanticAnalyzer") {
    val c = classOf[org.apache.hadoop.hive.ql.parse.SemanticAnalyzer]
    var m = c.getDeclaredMethod(
      "validateCreateView",
      classOf[org.apache.hadoop.hive.ql.plan.CreateViewDesc])
    m.setAccessible(true)
    assert(m.getReturnType === Void.TYPE)

    m = c.getDeclaredMethod(
      "convertRowSchemaToViewSchema",
      classOf[org.apache.hadoop.hive.ql.parse.RowResolver])
    m.setAccessible(true)
    assert(m.getReturnType === classOf[java.util.List[_]])

    val f = c.getDeclaredField("viewsExpanded")
    f.setAccessible(true)
    assert(f.getType === classOf[java.util.ArrayList[_]])
  }

  test("UnionOperator") {
    val c = classOf[org.apache.hadoop.hive.ql.exec.UnionOperator]
    var f = c.getDeclaredField("needsTransform")
    f.setAccessible(true)
    assert(f.getType === classOf[Array[Boolean]])
  }

  test("FileSinkOperator") {
    val fileSinkCls = classOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator]
    var f = fileSinkCls.getDeclaredField("fsp")
    f.setAccessible(true)
    assert(f.getType === classOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator#FSPaths])

    val fspCls  = classOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator#FSPaths]
    f = fspCls.getDeclaredField("finalPaths")
    f.setAccessible(true)
    assert(f.getType === classOf[Array[org.apache.hadoop.fs.Path]])
  }
}
