package br.ime.usp.sagat.test.integration

import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy.MessageStorageAndConsumptionPolicyParams
import br.ime.usp.sagat.test.mock.AutoAckMessageConsumer
import br.ime.usp.sagat.amqp.util.ControlStructures
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy._
import br.ime.usp.sagat.amqp.{ServerAMQPBridge, ClientAMQPBridge, AMQPBridge}
import java.util.concurrent.{CyclicBarrier, CountDownLatch, TimeUnit}

trait DefaultTestTemplate extends  ControlStructures{
  import AMQPBridge._
  import TimeUnit._

  private def basicSetup(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy):
  (CountDownLatch, CountDownLatch, ServerAMQPBridge, ClientAMQPBridge, IndexedSeq[Array[Byte]]) = {
      val latchServer = new CountDownLatch(count)
      val latchClient = new CountDownLatch(count)
      val clientConsumer = new AutoAckMessageConsumer("I am the client", verbose, latchClient)
      val serverConsumer = new AutoAckMessageConsumer("I am the server", verbose, latchServer)
      val server = newServerBridge(nodeName, serverConsumer, messageStorePolicy, policy)
      val client = newClientBridge(nodeName, clientConsumer, messageStorePolicy, policy)
      val messages = for(i <-1 to count) yield ("message %d" format i).getBytes
    (latchServer, latchClient, server, client, messages)
  }

  private def basicSetupForMultipleClients(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy, nClients: Int, serverReply: Boolean):
  (CountDownLatch, IndexedSeq[CountDownLatch], ServerAMQPBridge, IndexedSeq[ClientAMQPBridge], IndexedSeq[Array[Byte]]) = {
      val latchServer = new CountDownLatch(count * nClients)
      val latchClients = for(i <- 1 to nClients) yield new CountDownLatch(count)
      val clientConsumers = latchClients.map(new AutoAckMessageConsumer("I am one of the clients", verbose, _, false))
      val serverConsumer = new AutoAckMessageConsumer("I am the server", verbose, latchServer, serverReply)
      val server = newServerBridge(nodeName, serverConsumer, messageStorePolicy, policy)
      val clients = clientConsumers.map(newClientBridge(nodeName, _, messageStorePolicy, policy))
      val messages = for(i <-1 to count) yield ("message %d" format i).getBytes
    (latchServer, latchClients, server, clients, messages)
  }

  def defaultMultiClientSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy, nClients: Int): Long = {
    var result = 0L
    if(AMQP.enabled) executeAMQPWithSafeCleanUp {
        val (latchServer, latchClients, server, clients, messages) = basicSetupForMultipleClients(nodeName, count, verbose, messageStorePolicy, policy, nClients,
        false)
        clients.foreach(c => messages.foreach(c.sendMessageToServer))
        latchServer.await((count * nClients) / 100 + 1, SECONDS)
        result = latchServer.getCount
    }
    result
  }

  def defaultMultiClientSendTestWithServerReply(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy, nClients: Int): Long = {
    var result = 0L
    if(AMQP.enabled) executeAMQPWithSafeCleanUp {
        val (latchServer, latchClients, server, clients, messages) = basicSetupForMultipleClients(nodeName, count, verbose, messageStorePolicy, policy, nClients,
        true)
        clients.foreach(c => messages.foreach(c.sendMessageToServer))
        latchServer.await((count * nClients) / 100 + 1, SECONDS)
        result = latchServer.getCount

        latchClients.foreach(latch => latch.await(count / 100 + 1, SECONDS))
        result += latchClients.map(_.getCount).sum
    }
    result
  }

  def defaultClientSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy): Long = {
    var result = 0L
    if(AMQP.enabled) executeAMQPWithSafeCleanUp {
        val (latchServer, latchClient, server, client, messages) = basicSetup(nodeName, count, verbose, messageStorePolicy, policy)
        messages.foreach(client.sendMessageToServer)
        latchServer.await(count / 100 + 1, SECONDS)
        result = latchServer.getCount
    }
    result
  }

  def defaultServerSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy): Long = {
    var result = 0L
    if(AMQP.enabled) executeAMQPWithSafeCleanUp {
        val (_, latchClient, server, client, messages) = basicSetup(nodeName, count, verbose, messageStorePolicy, policy)
        /* as we have only one client, it this node, it is ok */
        val to = Some(client.id)
        messages.foreach(server.sendMessageTo(_, to))
        latchClient.await(count / 100 + 1, SECONDS)
        result = latchClient.getCount
    }
    result
  }


  def defaultServerFanoutSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy): Long = {
    var result = 0L
    if(AMQP.enabled) executeAMQPWithSafeCleanUp {
        val (_, latchClient, server, client, messages) = basicSetup(nodeName, count, verbose, messageStorePolicy, policy)
        messages.foreach(server.sendMessageTo(_, None))
        latchClient.await(count / 100 + 1, SECONDS)
        result = latchClient.getCount
    }
    result
  }
}