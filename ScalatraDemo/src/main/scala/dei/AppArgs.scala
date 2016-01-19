package dei

import javax.servlet.Servlet

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

import scala.util.Try

object AppArgs {

  def main(args : Array[String]) {
    // Parse args
    val DEFAULT_RESOURCE_ROOT = System.getProperty("user.dir") + "/static"
    val DEFAULT_PORT = 8089
    val DEFAULT_HBASE = "ht_tweets"

    val (port, root, htable) = args match {
      case Array(htable)      => (DEFAULT_PORT, DEFAULT_RESOURCE_ROOT, htable)
    }

    // Create server
    System.setProperty("org.eclipse.jetty.LEVEL","INFO")
    val server = new Server(port)


    // Register RealtimeServlet for demo 2
    registerServlet(server,
      resourceRoot = root,
      servletName = "app",
      servletUrlPath = "/app/*",
      servlet = new RealTimeServletHBase(), htable=htable.toString)



    // Start server
    server.start()
    server.join()
  }

  def registerServlet(server: Server,
                      resourceRoot:String,
                      servletName:String,
                      servletUrlPath: String,
                      servlet: Servlet, htable:String): Unit = {


    // Setup application "context" (handler tree in jetty speak)
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setResourceBase(resourceRoot)
    println("resourceRoot="+resourceRoot)

    // Path in URL to match
    context.setContextPath("/")
    server.setHandler(context)

    // Add custom servlet
    val holderDynamic = new ServletHolder(servletName, servlet)
    context.addServlet(holderDynamic, servletUrlPath)
    context.getServletContext().setAttribute("htable",htable)

    // Default servlet for root content (always last)
    val holderPwd = new ServletHolder("default", new DefaultServlet())
    holderPwd.setInitParameter("dirAllowed", "true")
    context.addServlet(holderPwd, "/")
  }

}
