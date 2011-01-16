package br.ime.usp.sagat.test.integration

import br.ime.usp.sagat.amqp.{StorageAndConsumptionPolicy}
import org.junit.{Test}
import org.scalatest.junit.JUnitSuite
import org.scalatest.matchers.ShouldMatchers
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy

class SingleClientAndServerInteractionWithExclusiveConsumersSpec extends JUnitSuite with ShouldMatchers with DefaultTestTemplate {
  import StorageAndConsumptionPolicy._
  import ConnectionSharePolicy._


  @Test
  def clientWithExclusiveConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultClientSendTest("node1", 50, false, EXCLUSIVE_FANOUT_TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE) should equal (0)


  @Test
  def serverWithExclusiveConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultServerFanoutSendTest("node2", 50, false, EXCLUSIVE_FANOUT_TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE) should equal (0)

  @Test
  def clientWithExclusiveConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultClientSendTest("node3", 50, false, EXCLUSIVE_FANOUT_TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL) should equal (0)

  @Test
  def serverWithExclusiveConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultServerFanoutSendTest("node4", 50, false, EXCLUSIVE_FANOUT_TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL) should equal (0)

}