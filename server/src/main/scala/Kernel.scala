package com.bwater.notebook
package server

import akka.actor._
import client._
import net.liftweb.json._
import JsonDSL._
import akka.dispatch.{Await, Promise}
import kernel.remote.{RemoteActorSystem, RemoteActorProcess}
import akka.util.duration._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import unfiltered.netty.websockets.WebSocket

/**
 * A kernel is a remote VM that connects to some local resources (for example, WebSockets).
 * The local resource must be fully initialized before we will let messages flow through
 * to the remote (this is accomplished by blocking on startup waiting for
 */

class Kernel(system: ActorSystem, initScripts: List[String], compilerArgs: List[String]) {
  implicit val executor = system.dispatcher

  val ioPubPromise = Promise[WebSockWrapper]
  val shellPromise = Promise[WebSockWrapper]
  val observablePromise = Promise[WebSocket]

  val l = system.actorOf(Props(new ExecutionManager))

  case object ShutdownNow

  def shutdown() { router ! ShutdownNow  }

  class ExecutionManager extends Actor with ActorLogging {

    // These get filled in before we ever receive messages
    var remoteInfo: RemoteActorSystem = null
    var calcService: ActorRef = null
    var obsService: ActorRef = null

    override def preStart() {
      remoteInfo = Await.result( RemoteActorSystem.spawn(system, "kernel"), 1 minutes)
      val iopub = Await.result(ioPubPromise.future, 2 minutes)
      val shell = Await.result(shellPromise.future, 2 minutes)
      val observable = Await.result(observablePromise.future, 2 minutes)
      calcService =  context.actorOf(Props(new CalcWebSocketService(initScripts, compilerArgs, iopub, shell, remoteInfo.deploy)))
      obsService = context.actorOf(Props(new ObservableRelay(observable, remoteInfo.deploy)))
    }
    override def postStop() {
      if (remoteInfo != null)
        remoteInfo.shutdownRemote()
    }

    def receive = {
      case calcMsg:CalcServiceMessage =>
        calcService forward calcMsg

      case obsMsg:ObservableMessage =>
        obsService forward obsMsg

      case ShutdownNow =>
      if (remoteInfo != null) {
        remoteInfo.shutdownRemote()
      }
    }
  }
}

object KernelManager {
  val kernels = new ConcurrentHashMap[String, Kernel]().asScala

  def get(id: String) = kernels.get(id)
  def apply(id: String) = kernels(id)

  def add(id:String, kernel: Kernel) {
    kernels += id -> kernel
  }
}

/**
 * Represents an indirect actor - one that might have an expensive initialization, and which
 *
 */
trait IndirectActorInfo {
  // Executes inside an actor in the KernelManager to create the desired actor
  def initialize(context: ActorContext): ActorRef

  def shouldRoute(msg: Any): Boolean
}
