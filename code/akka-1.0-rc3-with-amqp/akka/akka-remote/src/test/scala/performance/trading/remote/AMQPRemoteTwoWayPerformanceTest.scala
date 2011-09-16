package akka.performance.trading.remote


import performance.trading.remote.BaseRemoteTwoWayPerformanceTest
import org.junit.Test


class AMQPRemoteTwoWayPerformanceTest extends BaseRemoteTwoWayPerformanceTest {

  // need this so that junit will detect this as a test case
  @Test
  def dummy {}

  override def compareResultWith = Some("NettyRemoteTwoWayPerformanceTest")

}
