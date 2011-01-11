package br.ime.usp.sagat.mock

import java.util.concurrent.ConcurrentHashMap
import br.ime.usp.sagat.amqp.{MessageHandler, AMQPBridge}

class RemoteClientMock(val host: String, val port: Int) {
  require(host != null)
  require(port > 0)

  private var amqpClientBridge: AMQPBridge = null

  def connect{
    amqpClientBridge = AMQPBridge.newClientBridge(host + ":" + port, new ClientMessageConsumer)
  }

  def shutdown{
    amqpClientBridge.shutdown
  }

  def send(message: String){
    amqpClientBridge.sendMessage(message.getBytes)
  }
}


class ClientMessageConsumer extends MessageHandler {
  def process(message: Array[Byte]): (Boolean, Boolean) = {
    println("Client recebeu: " + new String(message))
    (true, false)
  }
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