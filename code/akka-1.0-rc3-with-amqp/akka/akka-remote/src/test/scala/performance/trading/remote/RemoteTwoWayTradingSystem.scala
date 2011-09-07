package akka.performance.trading.remote

import akka.actor.Actor._
import akka.performance.trading.oneway.{OneWayOrderReceiver}
import akka.actor.ActorRef
import akka.performance.trading.common.{Rsp, AkkaTradingSystem}
import akka.performance.trading.domain.Order

class RemoteTwoWayTradingSystem extends AkkaTradingSystem {

  override def useStandByEngines = false

  override def createOrderReceivers: List[ActorRef] = {
    val refs = (1 to 10).toList map (i ⇒ createOrderReceiver())
    val names = (1 to 10).toList map(n => "order-receiver-" + n)
    names.zip(refs).foreach {
      p => {
          if(remote.findActorById(p._1) != null) {
            remote.unregister(p._1)
          }
          p._2.start
          remote.register(p._1, p._2)
      }
    }
    names.map(remote.findActorById(_))
  }
}