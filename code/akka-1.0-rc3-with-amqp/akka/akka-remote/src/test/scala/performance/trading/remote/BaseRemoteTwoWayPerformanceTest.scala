package performance.trading.remote


import akka.actor.Actor._
import akka.performance.trading.common.AkkaPerformanceTest
import akka.performance.trading.common.Rsp
import akka.performance.trading.domain._
import akka.actor.ActorRef
import akka.performance.trading.remote.RemoteTwoWayTradingSystem
import akka.performance.trading.remote.RemoteSettings._

class BaseRemoteTwoWayPerformanceTest extends AkkaPerformanceTest {


  override def remote_? = true

  override def createTradingSystem: TS = new RemoteTwoWayTradingSystem {
    override def createMatchingEngine(meId: String, orderbooks: List[Orderbook]) = meDispatcher match {
      case Some(d) ⇒ {
         val act = actorOf(new TwoWayMatchingEngine(meId, orderbooks))
        act.setDispatcher(d)
        act
      }
      case _       ⇒ actorOf(new TwoWayMatchingEngine(meId, orderbooks))
    }
  }

  override def lookupRemoteTradingSystem: ActorRef = {
    remote.actorFor(serviceName, host, port)

  }

  override def placeOrder(orderReceiver: ActorRef, order: Order): Rsp = {
    (orderReceiver !! order).get.asInstanceOf[Rsp]
  }

}
