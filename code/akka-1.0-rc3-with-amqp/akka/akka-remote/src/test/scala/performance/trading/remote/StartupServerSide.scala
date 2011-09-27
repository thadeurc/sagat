package performance.trading.remote

import akka.actor.Actor._
import akka.performance.trading.remote.RemoteSettings._
import akka.performance.trading.remote.RemoteTwoWayTradingSystem
import org.scalatest.junit.JUnitSuite
import org.junit.Test

/**
 * Created by IntelliJ IDEA.
 * User: thadeu
 * Date: 9/25/11
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */

class StartupServerSide extends JUnitSuite {

  @Test
  def startupServer = {
    remote.start(host, port)
    remote.register(serviceName, actorOf[RemoteTwoWayTradingSystem])
  }
}