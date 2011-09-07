package performance.trading.remote

import akka.actor._
import akka.performance.trading.domain.Order
import akka.performance.trading.domain.Orderbook
import akka.event.EventHandler
import akka.performance.trading.common.{Rsp, AkkaMatchingEngine}

class TwoWayMatchingEngine(meId: String, orderbooks: List[Orderbook]) extends AkkaMatchingEngine(meId, orderbooks) {

  override def handleOrder(order: Order) {
    orderbooksMap.get(order.orderbookSymbol) match {
      case Some(orderbook) ⇒
        standby.foreach(_ ! order)
        orderbook.addOrder(order)
        orderbook.matchOrders()
        self.reply_?(new Rsp(true))
      case None ⇒
        EventHandler.warning(this, "Orderbook not handled by this MatchingEngine: " + order.orderbookSymbol)
        self.reply_?(new Rsp(false))
    }
  }

}
