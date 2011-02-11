package br.ime.usp.sagat.test.integration

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy.MessageStorageAndConsumptionPolicyParams
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy.ConnectionSharePolicyParams
import br.ime.usp.sagat.amqp.{AMQPBridge, ClientAMQPBridge, ServerAMQPBridge, MessageHandler}
import br.ime.usp.sagat.amqp.util.ControlStructures

object AMQP {
  val enabled = true
}

class AutoAckStringMessageConsumer(prefix: String, echo:Boolean = false, latch: CountDownLatch) extends MessageHandler {

  def handleRejectedMessage(message: Array[Byte], clientId: String) {
    println("rejected message sent to client %s".format(clientId))
  }

  def handleMessageReceived(message: Array[Byte]): Boolean = {
    latch.countDown
    val text = new String(message)
    if(echo) println("%s - Consumer received message [%s]" format(prefix, text))
    true
  }
}

trait DefaultBridgeTemplate {
  import AMQPBridge._
  import TimeUnit._

  private def basicSetup(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicyParams):
  (CountDownLatch, CountDownLatch, ServerAMQPBridge, ClientAMQPBridge, IndexedSeq[Array[Byte]]) = {
      val latchServer = new CountDownLatch(count)
      val latchClient = new CountDownLatch(count)
      val clientConsumer = new AutoAckStringMessageConsumer("I am the client", verbose, latchClient)
      val serverConsumer = new AutoAckStringMessageConsumer("I am the server", verbose, latchServer)
      val server = newServerBridge(nodeName, serverConsumer, messageStorePolicy, policy)
      val client = newClientBridge(nodeName, clientConsumer, messageStorePolicy, policy)
      val messages = for(i <-1 to count) yield ("message %d" format i).getBytes
    (latchServer, latchClient, server, client, messages)
  }

  private def basicSetupForMultipleClients(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicyParams, nClients: Int):
  (CountDownLatch, IndexedSeq[CountDownLatch], ServerAMQPBridge, IndexedSeq[ClientAMQPBridge], IndexedSeq[Array[Byte]]) = {
      val latchServer = new CountDownLatch(count * nClients)
      val latchClients = for(i <- 1 to nClients) yield new CountDownLatch(count)
      val clientConsumers = latchClients.map(new AutoAckStringMessageConsumer("I am one of the clients", verbose, _))
      val serverConsumer = new AutoAckStringMessageConsumer("I am the server", verbose, latchServer)
      val server = newServerBridge(nodeName, serverConsumer, messageStorePolicy, policy)
      val clients = clientConsumers.map(newClientBridge(nodeName, _, messageStorePolicy, policy))
      val messages = for(i <-1 to count) yield ("message %d" format i).getBytes
    (latchServer, latchClients, server, clients, messages)
  }

  def defaultMultiClientSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                    policy: ConnectionSharePolicyParams, nClients: Int): Long = {
      var result = 0L
      if(AMQP.enabled) {
        val (latchServer, latchClients, server, clients, messages) =
          basicSetupForMultipleClients(nodeName, count, verbose, messageStorePolicy, policy, nClients)
        latchServer.await(nClients, SECONDS)
        clients.foreach(c => messages.foreach(c.sendMessageToServer))
        latchServer.await((count * nClients) / 100 + 2, SECONDS)
        result = latchServer.getCount
        clients.foreach(c => c.shutdown)
        server.shutdown
      }
      result
  }

  def defaultMultiClientSendTestWithServerReply(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                    policy: ConnectionSharePolicyParams, nClients: Int): Long = {
      var result = 0L
      if(AMQP.enabled) {
          // TODO sem reply do server - precisa mexer
          val (latchServer, latchClients, server, clients, messages) = basicSetupForMultipleClients(nodeName, count, verbose, messageStorePolicy, policy, nClients)
          clients.foreach(c => messages.foreach(c.sendMessageToServer))
          latchServer.await((count * nClients) / 100 + 2, SECONDS)
          result = latchServer.getCount

          latchClients.foreach(latch => latch.await(count / 100 + 2, SECONDS))
          result += latchClients.map(_.getCount).sum
          server.shutdown
          clients.foreach(c => c.shutdown)
      }
      result
  }

  def defaultClientSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicyParams): Long = {
    var result = 0L
    if(AMQP.enabled) {
        val (latchServer, latchClient, server, client, messages) = basicSetup(nodeName, count, verbose, messageStorePolicy, policy)
        latchServer.await(1, SECONDS)
        messages.foreach(client.sendMessageToServer)
        latchServer.await(count / 100 + 2, SECONDS)
        result = latchServer.getCount
        server.shutdown
        client.shutdown
    }
    result
  }

  def defaultServerSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicyParams): Long = {
    var result = 0L
    if(AMQP.enabled) {
        val (_, latchClient, server, client, messages) = basicSetup(nodeName, count, verbose, messageStorePolicy, policy)
        val to = Some(client.id)
        latchClient.await(1, SECONDS)
        messages.foreach(server.sendMessageTo(_, to))
        latchClient.await(count / 100 + 2, SECONDS)
        result = latchClient.getCount
        server.shutdown
        client.shutdown
    }
    result
  }

  def defaultServerFanoutSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicyParams): Long = {
    var result = 0L
    if(AMQP.enabled) {
        val (_, latchClient, server, client, messages) = basicSetup(nodeName, count, verbose, messageStorePolicy, policy)
        messages.foreach(server.sendMessageTo(_, None))
        latchClient.await(count / 100 + 2, SECONDS)
        result = latchClient.getCount
        server.shutdown
        client.shutdown
    }
    result
  }
}