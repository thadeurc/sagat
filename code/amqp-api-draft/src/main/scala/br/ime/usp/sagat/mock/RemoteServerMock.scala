package br.ime.usp.sagat.mock


import java.util.concurrent.ConcurrentHashMap
import br.ime.usp.sagat.amqp.{StorageMode, AMQPBridge}
import java.lang.String
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope, ShutdownSignalException, Consumer}

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

    print("log: Attempt to start RemoteServerMock on %s:%s", host, port)

    if(!isRunning){
      register(host, port, this)
      amqpBridge = AMQPBridge.newServerAMQPBridge(host + ":" + port, new MessageConsumer, StorageMode.TRANSIENT)
      isRunning = true
      println(".... done")
    }else {
      println("....*failed*")
    }
  }

  def shutdown = {
    unregister(host, port)
  }

  def send(message: String): Unit = {}
}

class MessageConsumer extends Consumer {
  def handleDelivery(p1: String, p2: Envelope, p3: BasicProperties, message: Array[Byte]) = {
    println(new String(message))
  }

  def handleRecoverOk = {}

  def handleShutdownSignal(p1: String, p2: ShutdownSignalException) = {}

  def handleCancelOk(p1: String) = {}

  def handleConsumeOk(p1: String) = {}
}

object RemoteServerMock{

  private val remoteServers = new ConcurrentHashMap[String, RemoteServerMock]

  private def register(host: String, port: Int, server: RemoteServerMock) = remoteServers.put(host + port, server)

  private def serverFor(host: String, port: Int): Option[RemoteServerMock] = {
    val key = host + port
    val server = remoteServers.get(key)
    if(server eq null) None
    else Some(server)
  }

  private def unregister(host: String, port: Int) = {
    remoteServers.remove(host + port)
  }



}