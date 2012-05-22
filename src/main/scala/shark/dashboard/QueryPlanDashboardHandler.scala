package shark.dashboard

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.Request

import shark.exec.{ExplainTaskHelper, HiveOperator, Operator, TerminalAbstractOperator}
import spark.{CacheTracker, Utils}


/**
 * A dashboard for visualizing the query plan. It outputs the query plan in JSON
 * and enables the client side HTML page to illustrate the query plan (e.g. in
 * d3.js).
 */
class QueryPlanDashboardHandler extends AbstractHandler with shark.LogHelper {

  override def handle(
    target: String,
    baseRequest: Request,
    request: HttpServletRequest,
    response: HttpServletResponse)
  {
    val json = QueryPlanDashboardHandler.generateJson(
      QueryPlanDashboardHandler.terminalOperator)

    // Serve the HTTP content.
    response.setContentType("application/json;charset=utf-8")
    response.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    response.getWriter().println(json)
  }
}


object QueryPlanDashboardHandler {

  /**
   * A static variable for communicating the query plan between the parser and
   * this Dashboard. This is not the best design, but oh well .... :)
   */
  var terminalOperator: TerminalAbstractOperator[_] = _

  /**
   * Generates a JSON representation of the query plan.
   */
  def generateJson(op: Operator[_], indent: String = ""): String = {

    val parentJson = 
      if (op.parentOperators.size > 0) {
        op.parentOperators.map { p =>
          generateJson(p, indent + "    ")
        }.reduce(_ + ",\n" + _)
      } else {
        ""
      }

    val os = new java.io.ByteArrayOutputStream
    val ps = new java.io.PrintStream(os)
    ExplainTaskHelper.outputWork(
      op.hiveOp.asInstanceOf[HiveOperator].getConf.asInstanceOf[java.io.Serializable],
      ps,
      true,
      0)
    val details = os.toString().replace("\n", "\\n").replace("\"", "\\\"")

    indent + "{\n" + 
    indent + "  \"name\": \"" + op.getClass.getSimpleName + "\",\n" +
    indent + "  \"details\": \"" + details + "\",\n" +
    indent + "  \"contents\": [\n" + parentJson +
    indent + "  ]\n" +
    indent + "}\n"
  }
}