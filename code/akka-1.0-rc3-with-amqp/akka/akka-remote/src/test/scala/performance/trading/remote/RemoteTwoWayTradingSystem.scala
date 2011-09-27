package akka.performance.trading.remote

import akka.actor.Actor._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.performance.trading.common.{AkkaTradingSystem}
import RemoteSettings._

class RemoteTwoWayTradingSystem extends AkkaTradingSystem with Actor {

  override def useStandByEngines = false

  override def createOrderReceivers: List[ActorRef] = {
    (1 to 10).toList map (i ⇒ createOrderReceiver())
  }

  override def createRemoteOrderReceivers: List[String] = {
    val names = (1 to 10).toList map(n => "order-receiver-" + n)
    names.zip(localOrderReceivers).foreach {
      p => {
          if(remote.findActorById(p._1) != null) {
            remote.unregister(p._1)
          }
          p._2.start
          remote.register(p._1, p._2)
      }
    }
    names
  }

  def receive = {
    case "start" => {
      start
      self.reply_?(true)
    }
    case "shutdown" => {
      shutdown
      self.reply_?(true)
    }
    case "receivers" => {
      self.reply_?(orderReceiversNames)
    }
    case "warmupReceiver" => {
      self.reply_?(orderReceiversNames.head)
    }
  }
}