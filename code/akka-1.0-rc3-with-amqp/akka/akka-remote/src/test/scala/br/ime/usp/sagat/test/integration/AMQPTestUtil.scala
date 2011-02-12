package br.ime.usp.sagat.test.integration

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy.MessageStorageAndConsumptionPolicyParams
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy.ConnectionSharePolicyParams
import br.ime.usp.sagat.amqp._

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


class AutoAckAMQPSampleMessageConsumer(prefix: String, echo: Boolean = false, latch: CountDownLatch) extends MessageHandler {
  private [integration] var bridge: Option[AMQPBridge] = None

  import AMQPProtobufProtocol.AMQPSampleMessage._
  def handleRejectedMessage(message: Array[Byte], clientId: String) {
    println("rejected message sent to client %s".format(clientId))
  }

  def handleMessageReceived(message: Array[Byte]): Boolean = {
    latch.countDown
    val protobufMessage = parseFrom(message)
    val text = protobufMessage.getPayload
    if(echo)
      println("%s - Consumer received message [%s] from client [%s]" format(prefix, text,protobufMessage.getClientId))

    bridge.foreach{
      bd => bd.sendMessageTo(("reply of message [%s]".format(text)).getBytes, Some(protobufMessage.getClientId))
    }
    true
  }
}

trait DefaultBridgeTemplate {
  import AMQPBridge._
  import TimeUnit._
  import AMQPProtobufProtocol.AMQPSampleMessage._
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

  private def basicSetupForMultipleClientsWithReply(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicyParams, nClients: Int):
  (CountDownLatch, IndexedSeq[CountDownLatch], ServerAMQPBridge, IndexedSeq[(ClientAMQPBridge, IndexedSeq[Array[Byte]])]) = {
      val latchServer = new CountDownLatch(count * nClients)
      val latchClients = for(i <- 1 to nClients) yield new CountDownLatch(count)
      val clientConsumers = latchClients.map(new AutoAckStringMessageConsumer("I am one of the clients", verbose, _))
      val serverConsumer = new AutoAckAMQPSampleMessageConsumer("I am the server", verbose, latchServer)
      val server = newServerBridge(nodeName, serverConsumer, messageStorePolicy, policy)
      serverConsumer.bridge = Some(server)
      val clients = clientConsumers.map(newClientBridge(nodeName, _, messageStorePolicy, policy))
      val clientMessagesPair =
        clients.map{ aClient =>
          val messages = for(i <-1 to count) yield {
            newBuilder.setId(i).setClientId(aClient.id).setPayload("message %d" format i).build().toByteArray
          }
          (aClient, messages)
        }
    (latchServer, latchClients, server, clientMessagesPair)
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
          val (latchServer, latchClients, server, clientsMessagePair) = basicSetupForMultipleClientsWithReply(nodeName, count, verbose, messageStorePolicy, policy, nClients)
          latchServer.await(nClients, SECONDS)
          clientsMessagePair.foreach{
            line => {
              val aClient = line._1
              val itsMessages = line._2
              itsMessages.foreach(aClient.sendMessageToServer)
            }
          }
          latchServer.await((count * nClients) / 100 + 2, SECONDS)
          result = latchServer.getCount
          latchClients.foreach(latch => latch.await(count / 100 + 2, SECONDS))
          result += latchClients.map(_.getCount).sum
          server.shutdown
          clientsMessagePair.map{_._1}.foreach(c => c.shutdown)
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