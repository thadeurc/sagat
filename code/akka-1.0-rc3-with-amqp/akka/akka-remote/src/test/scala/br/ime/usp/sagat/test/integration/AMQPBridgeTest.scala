package br.ime.usp.sagat.test.integration

import org.scalatest.junit.JUnitSuite
import org.scalatest.matchers.ShouldMatchers
import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy
import br.ime.usp.sagat.amqp.util.{AMQPConnectionFactory, ConnectionSharePolicy}
import org.junit._

class AMQPBridgeTest extends JUnitSuite with ShouldMatchers  with DefaultBridgeTemplate {
  import StorageAndConsumptionPolicy._
  import ConnectionSharePolicy._

  @Test
  def clientWithConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultClientSendTest("node1", 100000, false, EXCLUSIVE_TRANSIENT, ONE_CONN_PER_CHANNEL) should equal (0)

  @Test
  def clientWithConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultClientSendTest("node3", 100000, false, EXCLUSIVE_TRANSIENT, ONE_CONN_PER_NODE) should equal (0)

  @Test
  def serverWithConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultServerSendTest("node2", 100000, false, EXCLUSIVE_TRANSIENT, ONE_CONN_PER_CHANNEL) should equal (0)

  @Test
  def serverWithConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultServerSendTest("node4", 100000, false, EXCLUSIVE_TRANSIENT, ONE_CONN_PER_NODE) should equal (0)

  @Test
  def multipleClientsSendingMessages = {
    defaultMultiClientSendTest("node5", 10000, false, EXCLUSIVE_TRANSIENT, ONE_CONN_PER_NODE, 10) should equal (0)
  }

}

object AMQPBridgeTest {
  @BeforeClass
  @AfterClass
  def cleanUp = {
    AMQPConnectionFactory.shutdownAll
  }
}