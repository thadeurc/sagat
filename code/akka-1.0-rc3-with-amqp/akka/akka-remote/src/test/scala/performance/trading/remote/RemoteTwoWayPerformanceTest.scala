package akka.performance.trading.remote



import org.junit.Test

import akka.actor.Actor._
import akka.performance.trading.common.AkkaPerformanceTest
import akka.performance.trading.common.Rsp
import akka.performance.trading.domain._
import akka.actor.{ActorRef}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import performance.trading.remote.TwoWayMatchingEngine

class RemoteTwoWayPerformanceTest extends AkkaPerformanceTest {

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

   override def placeOrder(orderReceiver: ActorRef, order: Order): Rsp = {
    (orderReceiver !! order).get.asInstanceOf[Rsp]
  }

  // need this so that junit will detect this as a test case
  @Test
  def dummy {}

  override def compareResultWith = None //Some("RspPerformanceTest")

}
