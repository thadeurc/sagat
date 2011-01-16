package br.ime.usp.sagat.test.integration

import org.scalatest.junit.JUnitSuite
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy
import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy
import org.scalatest.matchers.ShouldMatchers
import org.junit.Test


class SingleClientAndServerInteractionWithNonExclusiveConsumersSpec extends JUnitSuite with ShouldMatchers with DefaultTestTemplate {
  import StorageAndConsumptionPolicy._
  import ConnectionSharePolicy._

  @Test
  def clientWithConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultClientSendTest("node5", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL) should equal (0)


  @Test
  def serverWithConsumerAndTransientMessagesWithExclusiveConnectionSending =
    defaultServerSendTest("node6", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_CHANNEL) should equal (0)

  @Test
  def clientWithConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultClientSendTest("node7", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE) should equal (0)

  @Test
  def serverWithConsumerAndTransientMessagesWithSharedConnectionSending =
    defaultServerSendTest("node8", 20, false, TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE) should equal (0)
}