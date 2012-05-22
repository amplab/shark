package shark.dashboard

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.Request

import spark.{CacheTracker, SparkEnv, Utils}

/**
 * A dashboard to view the cache status on each slave nodes.
 */
class CacheTrackerDashboardHandler(
  val cacheTracker: CacheTracker = SparkEnv.get.cacheTracker)
  extends AbstractHandler {

  override def handle(
    target: String,
    baseRequest: Request,
    request: HttpServletRequest,
    response: HttpServletResponse)
  {
    // Get the cache status from tracker.
    val cacheStatus = cacheTracker.getCacheStatus()

    // Serve the HTTP content.
    response.setContentType("text/html;charset=utf-8")
    response.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    response.getWriter().println(
      <html>
        <head>
          <title>Shark Dashboard - Cache Status</title>
          <link rel="stylesheet" type="text/css" href="/static/dashboard.css" />
        </head>
        <body>
          <h1>Shark Cache Status</h1>
          <p>Master node: { System.getProperty("spark.master.host") }</p>
          <table border="1" cellspacing="0">
          <tr><th>Host</th><th>Capacity</th><th>Used</th><th>Used (%)</th></tr>
          {
            for ((host, capacity, use) <- cacheStatus) yield {
              val percent = (use * 100 / capacity).toInt

              <tr>
                <td class="node">{ host }</td>
                <td class="node">{ Utils.memoryBytesToString(capacity) }</td>
                <td class="node">{ Utils.memoryBytesToString(use) }</td>
                <td class="node">
                  <table border="1px" class="percent_bar"><tbody><tr>
                  <td cellspacing="0" class="percent_used" width={ percent + "%" }></td>
                  <td cellspacing="0" class="percent_unused" width={ (100 - percent) + "%" }></td>
                  </tr></tbody></table>
                </td>
              </tr>
            }
          }
          </table>
        </body>
      </html>
    )
  }

}

