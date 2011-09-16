package akka.performance.trading.remote


import org.junit.Test
import performance.trading.remote.BaseRemoteTwoWayPerformanceTest


class NettyRemoteTwoWayPerformanceTest extends BaseRemoteTwoWayPerformanceTest {

  // need this so that junit will detect this as a test case
  @Test
  def dummy {}

  override def compareResultWith = Some("AMQPRemoteTwoWayPerformanceTest")

}
