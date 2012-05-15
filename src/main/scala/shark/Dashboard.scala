package shark

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler, HandlerList, ResourceHandler}
import org.eclipse.jetty.server.{Request, Server}

import spark.{CacheTracker, SparkEnv, Utils}


object Dashboard extends LogHelper {

  private var server: Server = null
  private var port: Int = -1

  def start() {

    val staticFileBaseUrl = getClass.getResource("/dashboard")
    println(staticFileBaseUrl)
    val staticFileBase = staticFileBaseUrl.toString
    logInfo("Starting Jetty with static file base: %s".format(staticFileBase))

    port = System.getProperty("spark.dashboard.port", "8088").toInt
    server = new Server(port)

    // A resource handler for static files.
    val staticFileHandler = new ResourceHandler
    staticFileHandler.setResourceBase(staticFileBase)
    val contextHandler = new ContextHandler("/dashboard")
    contextHandler.setHandler(staticFileHandler)

    // DashboardHandler for dynamic dashboard.
    val dynamicHandler = new DashboardHandler(SparkEnv.get.cacheTracker)

    // Set the hanlders and start the http server.
    val handlerList = new HandlerList
    handlerList.setHandlers(Array(contextHandler, dynamicHandler))
    server.setHandler(handlerList)
    server.start()
  }
}


class DashboardHandler(val cacheTracker: CacheTracker) extends AbstractHandler {

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
          <title>Shark Dashboard</title>
          <link rel="stylesheet" type="text/css" href="/dashboard/dashboard.css" />
        </head>
        <body>
          <h1>Shark Dashboard</h1>
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

