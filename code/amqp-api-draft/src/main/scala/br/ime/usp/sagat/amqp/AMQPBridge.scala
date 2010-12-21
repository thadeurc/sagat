package br.ime.usp.sagat.amqp

import StorageMode._
import util.Logging
import com.rabbitmq.client._

/* TODO:
         3- tratamento de erros
         6- usar atores para controle de conexao aberta e como listeners?
*/


object AMQPBridge extends Logging {

  private lazy val connFactory = {
    // TODO estudar as vantagens de se usar outras propriedades
    // TODO isso deve ser lido de um property. tlvz colocar no akka.conf
    val cf = new ConnectionFactory
    cf.setHost("localhost")
    cf.setUsername("actor_admin")
    cf.setPassword("actor_admin")
    cf.setVirtualHost("/actor_host")
    cf
  }

  private var remoteServersConnection: Connection = null

  private var remoteClientConnection: Connection = null

  private[sagat] def getRemoteServerConnection: Connection = synchronized{
    if(remoteServersConnection == null){
      log.debug("Creating connection for RemoteServer")
      remoteServersConnection = connFactory.newConnection
    }
    log.debug("Getting connection to RemoteServer")
    remoteServersConnection
  }

  private[sagat] def getRemoteClientConnection: Connection = synchronized{
    if(remoteClientConnection == null){
      log.debug("Creating connection for RemoteClient")
      remoteClientConnection = connFactory.newConnection
    }
    log.debug("Getting connection to RemoteClient")
    remoteClientConnection
  }


  def newServerAMQPBridge(name: String, consumerListener: DefaultConsumer,
                          messageStoreMode: MessageStoreModeParams): AMQPBridge = {
    new AMQPBridgeServer(name, getRemoteServerConnection.createChannel)
                         .createExchange(messageStoreMode.exchangeParams)
                         .createAndBindQueues(messageStoreMode.queueParams)
                         .bindConsumerToQueue(consumerListener)

  }

  def newClientAMQPBridge(name: String, consumerListener: DefaultConsumer): AMQPBridge = {
    new AMQPBridgeClient(name, getRemoteClientConnection.createChannel).bindConsumerToQueue(consumerListener)
  }

}


abstract class AMQPBridge(val name: String, val channel: Channel) extends Logging{
  require(name != null)

  require(channel != null)

  private[amqp] lazy val exchangeName = "actor.exchange." + name

  private[amqp] lazy val inboundQueueName = "actor.queue.in." + name

  private[amqp] lazy val outboundQueueName = "actor.queue.out." + name

  private[amqp] lazy val routingKey_in = "default_in"

  private[amqp] lazy val routingKey_out = "default_out"

  private[amqp] def bindConsumerToQueue(consumerListener: Consumer): AMQPBridge

  def sendMessage(message: Array[Byte]): Unit
}

private[sagat] class AMQPBridgeClient(name: String, channel: Channel) extends AMQPBridge(name,channel){

  private[amqp] def bindConsumerToQueue(consumerListener: Consumer): AMQPBridgeClient = {
    require(consumerListener != null)
    log.debug("Binding consumer to {}", inboundQueueName)
    channel.basicConsume(inboundQueueName, true, consumerListener)
    this
  }

  def sendMessage(message: Array[Byte]): Unit = {
    val result = channel.basicPublish(exchangeName, routingKey_out, true, true, null, message)
      // TODO fazer algo com o resultado
  }
}

private[sagat] class AMQPBridgeServer(name: String, channel: Channel) extends AMQPBridge(name, channel){

  private[amqp] def bindConsumerToQueue(consumerListener: Consumer): AMQPBridgeServer = {
    require(consumerListener != null)
    log.debug("Binding consumer to {}", outboundQueueName)
    channel.basicConsume(outboundQueueName, true, consumerListener)
    this
  }

  def createExchange(params: ExchangeConfig.ExchangeParameters): AMQPBridgeServer = {
    log.debug("Creating Exchange {} ", Array(exchangeName, params))
    channel.exchangeDeclare(exchangeName,
                            params.typeConfig,
                            params.durable,
                            params.autoDelete,
                            params.arguments)
    this
  }

  def createAndBindInboundQueue(params: QueueConfig.QueueParameters): AMQPBridgeServer = {
    log.debug("Creating inbound queue {}", Array(inboundQueueName, params))
    channel.queueDeclare(inboundQueueName,
                         params.durable,
                         params.exclusive,
                         params.autoDelete,
                         params.arguments)
    log.debug("Binging inbound queue {}", Array(inboundQueueName, exchangeName, routingKey_in))
    channel.queueBind(inboundQueueName, exchangeName, routingKey_in)
    this
  }

  def createAndBindOutboundQueue(params: QueueConfig.QueueParameters): AMQPBridgeServer = {
    log.debug("Creating outbound queue {}", Array(outboundQueueName, params))
    channel.queueDeclare(outboundQueueName,
                         params.durable,
                         params.exclusive,
                         params.autoDelete,
                         params.arguments)
    log.debug("Binding outbound queue {} ", Array(outboundQueueName, exchangeName, routingKey_out))
    channel.queueBind(outboundQueueName, exchangeName, routingKey_out)
    this
  }



  def createAndBindQueues(params: QueueConfig.QueueParameters):AMQPBridgeServer = {
    createAndBindInboundQueue(params)
    createAndBindOutboundQueue(params)
    this
  }

  def sendMessage(message: Array[Byte]): Unit = {
    val result = channel.basicPublish(exchangeName, routingKey_in, true, true, null, message)
      // TODO fazer algo com o resultado
  }

}