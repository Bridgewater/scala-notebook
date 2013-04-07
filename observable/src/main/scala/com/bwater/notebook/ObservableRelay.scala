package com.bwater.notebook

import unfiltered.netty.websockets.WebSocket
import akka.actor.Actor
import net.liftweb.json._
import unfiltered.netty.websockets.WebSocket
import com.bwater.notebook.ObservableVMToBrowser

/**
 * Given a socket, relays messages from the remote (VM) to the socket
 */
class ObservableRelay(socket: WebSocket) extends Actor {
  def receive = {
    case msg:ObservableBrowserToVM =>
      handler ! msg
    case ObservableVMToBrowser(obsId, newValue) =>
      val respJson = ("id" -> obsId) ~ ("new_value" -> newValue)
      socket.send(pretty(render(respJson)))
  }

}
