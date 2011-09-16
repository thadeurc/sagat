package akka.performance.trading.remote

import akka.actor.Actor._
import akka.actor.ActorRef
import akka.performance.trading.common.{AkkaTradingSystem}
import RemoteSettings._

class RemoteTwoWayTradingSystem extends AkkaTradingSystem {

  override def useStandByEngines = false

  override def createOrderReceivers: List[ActorRef] = {
    (1 to 10).toList map (i ⇒ createOrderReceiver())
  }

  override def createRemoteOrderReceivers: List[ActorRef] = {
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
    names.map(remote.actorFor(_,host, port))
  }
}