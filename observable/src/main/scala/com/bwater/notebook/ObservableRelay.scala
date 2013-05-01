package com.bwater.notebook

import unfiltered.netty.websockets.WebSocket
import akka.actor._
import net.liftweb.json._
import JsonDSL._
import unfiltered.netty.websockets.WebSocket
import com.bwater.notebook.ObservableVMToBrowser
import akka.actor.Deploy
import unfiltered.netty.websockets.WebSocket
import com.bwater.notebook.ObservableVMToBrowser
import com.bwater.notebook.ObservableBrowserToVM

/**
 * Given a socket, relays messages from the remote (VM) to the socket
 */
class ObservableRelay(socket: WebSocket, remoteDeploy: Deploy) extends Actor with ActorLogging {
  var remote: ActorRef = null

  override def preStart() {
    remote = context.actorOf(Props(new ObservableRemote).withDeploy(remoteDeploy))
  }
  def receive = {
    case req:ObservableBrowserToVM =>
      remote forward req

    case ObservableVMToBrowser(obsId, newValue) =>
      val respJson = ("id" -> obsId) ~ ("new_value" -> newValue)
      socket.send(pretty(render(respJson)))
  }
}

class ObservableRemote extends Actor with ActorLogging {
  def receive = {
    case ObservableBrowserToVM(obsId, newValue) =>
    // TODO: This used to be wrapped with a try/catch. Still do that?
    log.info("Forwarding message to " + obsId)
    JSBus.forwardClientUpdateMessage(obsId, newValue)
  }
}
