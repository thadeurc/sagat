package br.ime.usp.sagat.mock


import java.util.concurrent.ConcurrentHashMap
import java.lang.String
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope, ShutdownSignalException, Consumer}
import br.ime.usp.sagat.amqp.{MessageHandler, StoragePolicy, AMQPBridge}

class RemoteServerMock{
  import RemoteServerMock._

  private var host: String = null
  private var port: Int = 0
  private var isRunning = false
  private var amqpBridge: AMQPBridge = null

  def start(host: String, port: Int){
    require(host != null)
    require(port > 0)

    this.host = host
    this.port = port

    print("log: Attempt to start RemoteServerMock on %s:%s".format(host, port))

    if(!isRunning){
      register(host, port, this)
      amqpBridge = AMQPBridge.newServerBridge(host + ":" + port, new ServerMessageConsumer, StoragePolicy.TRANSIENT)
      isRunning = true
    }
  }

  def shutdown = {
    amqpBridge.shutdown
    unregister(host, port)
  }

  def send(message: String): Unit = {
     amqpBridge.sendMessage(message.getBytes)
  }
}

class ServerMessageConsumer extends MessageHandler {
  def process(message: Array[Byte]): (Boolean, Boolean) = {
    println("Server recebeu:"+new String(message))
    (true, false)
  }
}

object RemoteServerMock{

  private val remoteServers = new ConcurrentHashMap[String, RemoteServerMock]

  private def register(host: String, port: Int, server: RemoteServerMock) = remoteServers.put(host + port, server)

  def serverFor(host: String, port: Int): RemoteServerMock = {
    val key = host + port
    var server = remoteServers.get(key)
    if(server eq null){
      server = new RemoteServerMock
      server.start(host, port)
      register(host, port, server)
    }
    server
  }

  private def unregister(host: String, port: Int) = {
    remoteServers.remove(host + port)

  }



}