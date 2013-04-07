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

/**
 * Handles all kernel requests.
 */

class Kernel(system: ActorSystem, initScripts: List[String], compilerArgs: List[String]) {
  implicit val executor = system.dispatcher
  val ioPubPromise = Promise[WebSockWrapper]
  val shellPromise = Promise[WebSockWrapper]

  val executionManager = system.actorOf(Props(new ExecutionManager))

  case object ShutdownNow

  def shutdown() { executionManager ! ShutdownNow  }

  class ExecutionManager extends Actor with ActorLogging {

    // These get filled in before we ever receive messages
    var remoteInfo: RemoteActorSystem = null
    var calcService: ActorRef = null

    override def preStart() {
      remoteInfo = Await.result( RemoteActorSystem.spawn(system, "kernel"), 1 minutes)
      val iopub = Await.result(ioPubPromise.future, 5 minutes)
      val shell = Await.result(shellPromise.future, 5 minutes)
      calcService =  context.actorOf(Props(new CalcWebSocketService(initScripts, compilerArgs, iopub, shell, remoteInfo.deploy)))
    }
    override def postStop() {
      if (remoteInfo != null)
        remoteInfo.shutdownRemote()
    }

    def receive = {
      case calcMsg:CalcServiceMessage =>
        calcService forward calcMsg

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