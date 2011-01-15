package br.ime.usp.sagat.test.integration

import org.scalatest.junit.JUnitSuite
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy
import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy
import org.scalatest.matchers.ShouldMatchers
import org.junit.Test


class ClientAndServerInteractionWithNonExclusiveConsumersSpec extends JUnitSuite with ShouldMatchers with DefaultTestTemplate {
  import StorageAndConsumptionPolicy._
  import ConnectionSharePolicy._

  @Test
  def clientWithConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultSendTest("node5", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL, true) should equal (0)


  @Test
  def serverWithConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultSendTest("node6", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL, false) should equal (0)

  @Test
  def clientWithConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultSendTest("node7", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE, true) should equal (0)

  @Test
  def serverWithConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultSendTest("node8", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE, false) should equal (0)
}