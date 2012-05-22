package shark.dashboard

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.Request

import shark.exec.{Operator, TerminalAbstractOperator}
import spark.{CacheTracker, Utils}


/**
 * A dashboard for visualizing the query plan.
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
  var terminalOperator: TerminalAbstractOperator[_] = _

  def generateJson(op: Operator[_], indent: String = ""): String = {
    val parentJson = 
      if (op.parentOperators.size > 0) {
        op.parentOperators.map(generateJson(_, indent + "    ")).reduce(_ + ",\n" + _)
      } else {
        ""
      }

    indent + "{\n" + 
    indent + "  name: \"" + op + "\",\n" +
    indent + "  details: \"\"\n" +
    indent + "  contents: [\n" + parentJson +
    indent + "  ]\n" +
    indent + "}\n"
  }
}