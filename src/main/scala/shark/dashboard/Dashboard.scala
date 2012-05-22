package shark.dashboard

import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler, HandlerList, ResourceHandler}
import org.eclipse.jetty.server.Server

/**
 * The basic object for the Shark HTTP UI. Call start() to launch the HTTP
 * service. This is backed by Jetty.
 */
object Dashboard extends shark.LogHelper {

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

    // Handlers to register. Wildcard handlers must go last.
    val handlers = Array[(String, AbstractHandler)](
      ("/static", staticFileHandler),
      ("/queryPlan", new QueryPlanDashboardHandler),
      ("*", new CacheTrackerDashboardHandler)
    )

    val handlersToRegister = handlers.map { case(path, handler) =>
      if (path == "*") {
        handler
      } else {
        val contextHandler = new ContextHandler(path)
        contextHandler.setHandler(handler)
        contextHandler.asInstanceOf[org.eclipse.jetty.server.Handler]
      }
    }

    // Set the handlers and start the http server.
    val handlerList = new HandlerList
    handlerList.setHandlers(handlersToRegister)
    server.setHandler(handlerList)
    server.start()
  }
}

