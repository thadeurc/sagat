package br.ime.usp.sagat.mock

import java.util.concurrent.ConcurrentHashMap
import br.ime.usp.sagat.amqp.AMQPBridge
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{ShutdownSignalException, Consumer, Envelope}

class RemoteClientMock(val host: String, val port: Int) {
  require(host != null)
  require(port > 0)

  private var amqpClientBridge: AMQPBridge = null

  def connect{
    amqpClientBridge = AMQPBridge.newClientBridge(host + ":" + port, new ClientMessageConsumer)
  }

  def shutdown{

  }

  def send(message: String){
    amqpClientBridge.sendMessage(message.getBytes)
  }
}


class ClientMessageConsumer extends Consumer {
  def handleDelivery(p1: String, p2: Envelope, p3: BasicProperties, message: Array[Byte]) = {
    println(new String(message))
  }

  def handleRecoverOk = {}

  def handleShutdownSignal(p1: String, p2: ShutdownSignalException) = {}

  def handleCancelOk(p1: String) = {}

  def handleConsumeOk(p1: String) = {}
}

object RemoteClientMock {
  private val remoteClients = new ConcurrentHashMap[String,RemoteClientMock]()

  def clientFor(host: String, port: Int): RemoteClientMock = synchronized{
    val key = host + port
    if(remoteClients.containsKey(key)) remoteClients.get(key)
    else {
      val client = new RemoteClientMock(host, port)
      client.connect
      remoteClients.put(key, client)
      client
    }
  }
}