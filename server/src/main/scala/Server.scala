package com.bwater.notebook

import org.apache.commons.io.FileUtils
import org.jboss.netty.handler.stream.ChunkedWriteHandler
import unfiltered.netty.Http
import unfiltered.netty.Resources
import com.bwater.notebook.util.Logging
import org.apache.log4j.PropertyConfigurator
import server._
import java.io.{ IOException, File, Reader }
import java.net.{ InetAddress, URLEncoder }
import com.typesafe.config.Config
import java.io.BufferedReader
import com.typesafe.config.ConfigFactory
import java.io.FileReader
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigSyntax

/**
 * Author: Ken
 */

/**embedded server */
object Server extends Logging {

  FileUtils.forceMkdir(new File("logs"))

  private def openBrowser(url: String) {
    println("Launching browser on %s".format(url))
    unfiltered.util.Browser.open(url) match {
      case Some(ex) => println("Cannot open browser to %s\n%s".format(url, ex.toString))
      case None =>
    }
  }

  private def nullStartAction(url: String): Unit = ()

  def main(args: Array[String]) {
    startServer(args, ScalaNotebookConfig.withOverrides(ScalaNotebookConfig.defaults))
  }

  // This is basically unfiltered.util.Port.any with a preferred port, and is host-aware. Like the original, this
  // approach can be really unlucky and have someone else steal our port between opening this socket and when unfiltered
  // opens it again, but oh well...
  private def choosePort(host: String, preferredPort:Int) = {
    val addr = InetAddress.getByName(host)

    // 50 for the queue size is java's magic number, not mine. The more common ServerSocket constructor just
    // specifies it for you, and we need to pass in addr so we pass in the magic number too.
    val s = try {
      new java.net.ServerSocket(preferredPort, 50, addr)
    } catch {
      case ex: IOException =>
        new java.net.ServerSocket(0, 50, addr)
    }
    val p = s.getLocalPort
    s.close()
    p
  }


  def startServer(args: Array[String], config: ScalaNotebookConfig) {
    PropertyConfigurator.configure(getClass.getResource("/log4j.server.properties"))
    logDebug("Classpath: " + System.getProperty("java.class.path"))

    val argslist = args.mkString(" ").replaceAll("=", " ").split(" ").toList
    type OptionMap = Map[String, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--disable_security" :: tail =>
                               nextOption(map ++ Map("disable_security" -> true), tail)
        case "--host" :: value :: tail =>
                               nextOption(map ++ Map("host" -> value), tail)
        case "--port" :: value :: tail =>
                               nextOption(map ++ Map("port" -> value.toInt), tail)
        case "--notebook" :: value :: tail =>
                               nextOption(map ++ Map("notebook" -> value), tail)
        case "--start_action" :: value :: tail =>
                               nextOption(map ++ Map("start_action" -> value), tail)
        case other        :: tail => println("Unknown commandline option "+ other)
                               nextOption(map, tail)
      }
    }
    val defaults = Map("disable_security" -> false,
                       "host" -> "127.0.0.1",
                       "port" -> 8899,
                       "start_action" -> "open_browser")

    val options = nextOption(defaults, argslist)
    val secure = !options("disable_security").toString.toBoolean
    val host = options("host").toString
    val port = choosePort(host, options("port").asInstanceOf[Int])
    val notebook = options.getOrElse("notebook", "").toString

    logInfo("Running SN Server in " + config.notebooksDir.getAbsolutePath)
    val security = if (secure) new ClientAuth(host, port) else Insecure

    val queryString =
      for (name <- notebook)
      yield "?dest=" + URLEncoder.encode("/view/" + name, "UTF-8")

    val startActionOption = options("start_action")
    val startAction: (String) => Unit = startActionOption match {
      case "open_browser" => openBrowser
      case "none" => nullStartAction
      case _ =>
        println(s"Unknown start action: '$startActionOption'.")
        nullStartAction
    }

    startServer(config, host, port, security) {
      val baseUrl = "http://%s:%d/%s".format(host, port, security.loginPath)
      (http, app) => startAction((baseUrl ++ queryString).mkString)
    }
  }

  /* TODO: move host, port, security settings into config? */
  def startServer(config: ScalaNotebookConfig, host: String, port: Int, security: DispatcherSecurity)(startAction: (Http, Dispatcher) => Unit) {

    if (!config.notebooksDir.exists()) {
      logWarn("Base directory %s for Scala Notebook server does not exist.  Creating, but your server may be misconfigured.".format(config.notebooksDir))
      config.notebooksDir.mkdirs()
    }

    val app: Dispatcher = new Dispatcher(config, host, port)
    import security.{ withCSRFKey, withCSRFKeyAsync, withWSAuth, authIntent }

    val wsPlan = unfiltered.netty.websockets.Planify(withWSAuth(app.WebSockets.intent)).onPass(_.sendUpstream(_))

    val authPlan = unfiltered.netty.cycle.Planify(authIntent)

    val nbReadPlan = unfiltered.netty.cycle.Planify(withCSRFKey(app.WebServer.nbReadIntent))
    val nbWritePlan = unfiltered.netty.cycle.Planify(withCSRFKey(app.WebServer.nbWriteIntent))
    val templatesPlan = unfiltered.netty.cycle.Planify(app.WebServer.otherIntent)
    val kernelPlan = unfiltered.netty.async.Planify(withCSRFKeyAsync(app.WebServer.kernelIntent))
    val loggerPlan = unfiltered.netty.cycle.Planify(new ReqLogger().intent)

    val obsInt = unfiltered.netty.websockets.Planify(withWSAuth(new ObservableIntent(app.system).webSocketIntent)).onPass(_.sendUpstream(_))

    val iPythonRes = Resources(getClass.getResource("/from_ipython/"), 3600, true)
    val thirdPartyRes = Resources(getClass.getResource("/thirdparty/"), 3600, true)

    //TODO: absolute URL's may not be portable, should they be supported?  If not, are resources defined relative to notebooks dir or module root?
    def userResourceURL(res: File) = {
      if (res.isAbsolute()) res.toURI().toURL()
      else new File(config.notebooksDir, res.getPath()).toURI().toURL()
    }
    val moduleRes = config.serverResources map (res => Resources(userResourceURL(res), 3600, true))
    val observableRes = Resources(getClass.getResource("/observable/"), 3600, false)

    val http = unfiltered.netty.Http(port)

    class Pipe[A](value: A) {
      def pipe[B](f: A => B): B = f(value)
    }
    implicit def Pipe[A](value: A) = new Pipe(value)

    def resourcePlan(res: Resources*)(h: Http) = res.foldLeft(h)((h, r) => h.plan(r).makePlan(new ChunkedWriteHandler))

    http
      .handler(obsInt)
      .handler(wsPlan)
      .chunked(256 << 20)
      .handler(loggerPlan)

      .handler(authPlan)

      .handler(nbReadPlan)
      .handler(nbWritePlan)
      .handler(kernelPlan)
      .handler(templatesPlan)

      /* Workaround for https://github.com/unfiltered/unfiltered/issues/139 */
      .pipe(resourcePlan(iPythonRes, thirdPartyRes))
      .pipe(resourcePlan(moduleRes: _*))
      .pipe(resourcePlan(observableRes))
      .run({
        svr =>
          startAction(svr, app)
      }, {
        svr =>
          logInfo("shutting down server")
          KernelManager.shutdown()
          app.system.shutdown()
      })
  }
}
