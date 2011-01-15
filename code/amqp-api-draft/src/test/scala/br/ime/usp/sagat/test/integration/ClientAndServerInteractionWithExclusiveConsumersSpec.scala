package br.ime.usp.sagat.test.integration

import br.ime.usp.sagat.amqp.{StorageAndConsumptionPolicy}
import org.junit.{Test}
import org.scalatest.junit.JUnitSuite
import org.scalatest.matchers.ShouldMatchers
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy

class ClientAndServerInteractionWithExclusiveConsumersSpec extends JUnitSuite with ShouldMatchers with DefaultTestTemplate {
  import StorageAndConsumptionPolicy._
  import ConnectionSharePolicy._


  @Test
  def clientWithExclusiveConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultSendTest("node1", 500000, false, EXCLUSIVE_TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE, true) should equal (0)


  @Test
  def serverWithExclusiveConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultSendTest("node2", 500000, false, EXCLUSIVE_TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE, false) should equal (0)

  @Test
  def clientWithExclusiveConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultSendTest("node3", 500000, false, EXCLUSIVE_TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL, true) should equal (0)

  @Test
  def serverWithExclusiveConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultSendTest("node4", 500000, false, EXCLUSIVE_TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL, false) should equal (0)

}