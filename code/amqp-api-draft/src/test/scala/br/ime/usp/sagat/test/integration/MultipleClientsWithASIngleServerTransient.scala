package br.ime.usp.sagat.test.integration

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitSuite
import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy
import org.junit.Test


class MultipleClientsWithASIngleServerTransient extends JUnitSuite with ShouldMatchers with DefaultTestTemplate {
  import StorageAndConsumptionPolicy._
  import ConnectionSharePolicy._

  @Test
  def MultipleClientsSendingMessages = {
    defaultMultiClientSendTest("node9", 50, false, EXCLUSIVE_TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE, 5) should equal (0)
  }

  @Test
  def MultipleClientsExchangingMessagesWithServer = {
    defaultMultiClientSendTestWithServerReply("node10", 50, false, EXCLUSIVE_TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE, 5) should equal (0)
  }
}