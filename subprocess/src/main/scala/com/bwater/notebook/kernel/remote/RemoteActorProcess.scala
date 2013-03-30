/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */
package com.bwater.notebook.kernel.remote

import com.bwater.notebook.kernel.pfork.{ProcessInfo, BetterFork, ForkableProcess}
import akka.actor._
import com.typesafe.config.ConfigFactory
import akka.remote.{RemoteScope, RemoteActorRefProvider}
import akka.dispatch.Future
import java.io.File
import org.apache.commons.io.FileUtils

/**
 * Author: Ken
 */
class RemoteActorProcess extends ForkableProcess{
  // http://stackoverflow.com/questions/14995834/programmatically-obtain-ephemeral-port-with-akka
  var _system: ActorSystem = null

  def init(args: Seq[String]): String = {
    val Seq(configFile, cookieFile) = args
    val cookie = FileUtils.readFileToString(new File(cookieFile))
    val cfg = ConfigFactory.load(configFile)
    _system = ActorSystem("Remote", AkkaConfigUtils.requireCookie(cfg, cookie))
    // TODO: In akka 2.1, just use _system.provider.getDefaultAddress

    val address = GetAddress(_system).address
    println(address)
    address.toString
//    address.port.get?OrElse(sys.error("not a remote actor system: %s".format(cfg))).toString
  }

  def waitForExit() {
    _system.awaitTermination()
  }
}

class FindAddressImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider match {
    case rarp: RemoteActorRefProvider => rarp.transport.address
    case _ => system.provider.rootPath.address
  }
}

object GetAddress extends ExtensionKey[FindAddressImpl]
case object RemoteShutdown

class ShutdownActor extends Actor {
  protected def receive = {
    case RemoteShutdown => context.system.shutdown()
  }

}

/**
 * Represents a running remote actor system, with an address and the ability to kill it
 */
class RemoteActorSystem(localSystem: ActorSystem, info: ProcessInfo) {
  val address = AddressFromURIString(info.initReturn)
  val shutdownActor = localSystem.actorOf(Props(new ShutdownActor).withDeploy(Deploy(scope = RemoteScope(address))))

  def actorOf(context: ActorRefFactory, props: Props) = context.actorOf(props.withDeploy(Deploy(scope = RemoteScope(address))))

  def deploy = Deploy(scope = RemoteScope(address))

  def shutdownRemote() { shutdownActor ! RemoteShutdown }
  def killRemote() { info.kill() }

}

/**
 * Create a remote actor system
 */
object RemoteActorSystem {
  def spawn(system: ActorSystem, configFile:String): Future[RemoteActorSystem] = {
    //TODO: cookie file should only be written once.
    val akkaCookieFile = new File(".", ".akka-cookie")
    for (cookie <- AkkaConfigUtils.requiredCookie(system.settings.config)) {
      FileUtils.writeStringToFile(akkaCookieFile, cookie)
    }
    new BetterFork[RemoteActorProcess](system.dispatcher).execute(configFile, akkaCookieFile.getAbsolutePath) map { new RemoteActorSystem(system, _) }
  }
//  def apply(system: ActorSystem, configFile:String, props: Props): Future[ActorRef] = apply(system, configFile) map { s => system.actorOf(props.withDeploy(s.deploy)) }
}
