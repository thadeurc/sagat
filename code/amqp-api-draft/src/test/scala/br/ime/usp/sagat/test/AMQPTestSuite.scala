package br.ime.usp.sagat.test

import org.scalatest.junit.{ShouldMatchersForJUnit, JUnitSuite}
import org.junit.{After, Test, Before}
import br.ime.usp.sagat.amqp._

class AMQPTestSuite extends JUnitSuite with ShouldMatchersForJUnit with DataHelper{
  import ExchangeConfig._
  import AMQPBridge._
  import QueueConfig._

  var servers = generateServers(5)

  @Before
  def setup = {
  }

  @Test
  def createTransientAutoDeleteExchange = {
    servers.foreach(s => s.createExchange(exchangeNotDurableAutoDelete))
    servers.foreach(s => s.disconnect)
    // TODO tem que fazer algo para fechar a conexao caso nao haja mais channels abertos - usar os listeners.. preciso entender melhor o protocolo
    lock.synchronized{
      servers(0).channel.getConnection.close
    }
    servers = generateServers(5)
    servers.foreach(s => s.createExchange(exchangeNotDurableAutoDelete))
    servers.foreach(excludeExchange)
    servers.foreach(s => s.disconnect)
    lock.synchronized{
      servers(0).channel.getConnection.close
    }

  }

  @Test
  def createTransientAutoDeleteQueues = {
    servers.foreach(s => s.createExchange(exchangeNotDurableAutoDelete))
    servers.foreach(s => s.createAndBindQueues(queueNotDurableAutoDelete))
    servers.foreach(s => s.disconnect)
    lock.synchronized{
      servers(0).channel.getConnection.close
    }
    servers = generateServers(5)
    servers.foreach(s => s.createExchange(exchangeNotDurableAutoDelete))
    servers.foreach(s => s.createAndBindQueues(queueNotDurableAutoDelete))
    servers.foreach(excludeExchange)
    servers.foreach(excludeInAndOutQueues)
    servers.foreach(s => s.disconnect)
    lock.synchronized{
      servers(0).channel.getConnection.close
    }

  }

  @Test
  def serverTransientReceiveMessageFromClient = {
    val server = newServerAMQPBridge("myserverX", defaultConsumer, StorageMode.TRANSIENT)
    val client = newClientAMQPBridge("myserverX", defaultFakeConsumer)
    messagesToSend.foreach(m => client.sendMessage(m.getBytes))
    try{
      println("waiting...")
      Thread.sleep(1000)
    }catch{
      case e: InterruptedException =>
    }
    excludeAll(server)
    server.disconnect
    client.disconnect
    assert(defaultConsumer.difference.isEmpty)
  }

  @Test
  def clientReceiveMessageFromServerTransient = {
    val server = newServerAMQPBridge("myserverX", defaultFakeConsumer, StorageMode.TRANSIENT)
    val client = newClientAMQPBridge("myserverX", defaultConsumer)
    messagesToSend.foreach(m => server.sendMessage(m.getBytes))
    try{
      println("waiting...")
      Thread.sleep(1000)
    }catch{
      case e: InterruptedException =>
    }
    excludeAll(server)
    server.disconnect
    client.disconnect
    assert(defaultConsumer.difference.isEmpty)
  }


  @After
  def cleanUp = {
    try{
      servers.foreach(excludeInAndOutQueues)
    }catch {
      case e: Throwable =>
    }
    try{
      servers.foreach(excludeExchange)
    } catch {
      case e: Throwable =>
    }
    try{
      servers.foreach(s => s.disconnect)
    }catch{
      case e: Throwable =>
    }
    try{
      lock.synchronized{
        servers(0).channel.getConnection.close
      }
    }catch{
      case e: Throwable =>
    }
    servers = null

  }


  def excludeAll(server: AMQPBridge){
    excludeInAndOutQueues(server)
    excludeExchange(server)
  }

  def excludeInAndOutQueues(server: AMQPBridge) {
    server.channel.queuePurge(server.inboundQueueName)
    server.channel.queuePurge(server.outboundQueueName)
    server.channel.queueDelete(server.inboundQueueName)
    server.channel.queueDelete(server.outboundQueueName)
  }

  def excludeExchange(server:AMQPBridge) {
    server.channel.exchangeDelete(server.exchangeName)
  }

  def generateServers(count: Int): List[AMQPBridgeServer] = {
    val r = for(i <- 1 to count) yield new AMQPBridgeServer("myserver"+i, getRemoteServerConnection.createChannel)
    r.toList
  }
}