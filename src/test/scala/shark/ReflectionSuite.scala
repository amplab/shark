package shark

import org.scalatest.FunSuite


/**
 * A suite of test to ensure reflections are used properly in Shark to invoke
 * Hive non-public methods. This is needed because we cannot detect reflection
 * errors until runtime. Every time reflection is used to expand visibility of
 * methods or variables, a test should be added.
 */
class ReflectionSuite extends FunSuite {

  test("Driver") {
    val c = classOf[org.apache.hadoop.hive.ql.Driver]

    var m = c.getDeclaredMethod(
      "doAuthorization", classOf[org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer])
    m.setAccessible(true)
    assert(m.getReturnType === Void.TYPE)

    m = c.getDeclaredMethod("getSemanticAnalyzerHooks")
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
  }

  test("SemanticAnalyzer") {
    val c = classOf[org.apache.hadoop.hive.ql.parse.SemanticAnalyzer]
    var m = c.getDeclaredMethod(
      "validateCreateTable",
      classOf[org.apache.hadoop.hive.ql.plan.CreateTableDesc])
    m.setAccessible(true)
    assert(m.getReturnType === Void.TYPE)

    m = c.getDeclaredMethod(
      "convertRowSchemaToViewSchema",
      classOf[org.apache.hadoop.hive.ql.parse.RowResolver])
    m.setAccessible(true)
    assert(m.getReturnType === classOf[java.util.List[_]])
  }

  test("ExecDriver") {
    val c = classOf[org.apache.hadoop.hive.ql.exec.ExecDriver]
    var m = c.getDeclaredMethod(
      "getResourceFiles",
      classOf[org.apache.hadoop.conf.Configuration],
      classOf[org.apache.hadoop.hive.ql.session.SessionState.ResourceType])
    m.setAccessible(true)
    assert(m.getReturnType === classOf[String])
  }

  test("UnionOperator") {
    val c = classOf[org.apache.hadoop.hive.ql.exec.UnionOperator]
    var f = c.getDeclaredField("needsTransform")
    f.setAccessible(true)
    assert(f.getType === classOf[Array[Boolean]])
  }
}
