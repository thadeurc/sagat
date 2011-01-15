package br.ime.usp.sagat.test.integration

import br.ime.usp.sagat.amqp.StorageAndConsumptionPolicy.MessageStorageAndConsumptionPolicyParams
import br.ime.usp.sagat.test.mock.AutoAckMessageConsumer
import java.util.concurrent.{CountDownLatch, TimeUnit}
import br.ime.usp.sagat.amqp.util.ControlStructures
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy._
import br.ime.usp.sagat.amqp.AMQPBridge

trait DefaultTestTemplate extends  ControlStructures{
  import AMQPBridge._
  import TimeUnit._
  def defaultSendTest(nodeName: String, count: Int, verbose: Boolean, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                  policy: ConnectionSharePolicy, clientSend: Boolean): Int = {

    if(AMQP.enabled) executeAMQPWithSafeCleanUp {
      val latchServer = new CountDownLatch(count)
      val latchClient = new CountDownLatch(count)
      val clientConsumer = new AutoAckMessageConsumer("I am the client", verbose, latchClient)
      val serverConsumer = new AutoAckMessageConsumer("I am the server", verbose, latchServer)
      val server = newServerBridge(nodeName, serverConsumer, messageStorePolicy, policy)
      val client = newClientBridge(nodeName, clientConsumer, messageStorePolicy, policy)
      val messages = for(i <-1 to count) yield ("message %d" format i).getBytes

      if(clientSend){
        messages.foreach(client.sendMessage)
        latchServer.await(count / 100 + 1, SECONDS)
        latchServer.getCount
      }
      else{
        messages.foreach(server.sendMessage)
        latchClient.await(count / 100 + 1, SECONDS)
        latchClient.getCount
      }
    }
    0
  }
}