package br.ime.usp.sagat.amqp

import StorageAndConsumptionPolicy._
import com.rabbitmq.client._
import util.{EnhancedConnection, ControlStructures, Logging}
import util.ConnectionSharePolicy._
import util.ConnectionPool._
import com.rabbitmq.client.AMQP.BasicProperties
import java.lang.String

trait MessageHandler {
  def process(message: Array[Byte]): (Boolean, Boolean)
}

object AMQPBridge extends Logging {

  def newServerBridge(name: String, handler: MessageHandler): AMQPBridge = {
    newServerBridge(name, handler,  TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams): AMQPBridge = {
    newServerBridge(name, handler, messageStorePolicy, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicy): AMQPBridge = {
    newServerBridge(name, handler, TRANSIENT_AUTODELETE, policy)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams, policy: ConnectionSharePolicy): AMQPBridge = {
    new ServerAMQPBridge(name, getConnectionForServerBridge(name, policy))
                         .createExchange(messageStorePolicy.exchangeParams)
                         .createAndBindQueues(messageStorePolicy.queueParams)
                         .bindConsumerToQueue(handler)
  }

  def newClientBridge(name: String, handler: MessageHandler): AMQPBridge = {
    newClientBridge(name, handler, ONE_CONN_PER_NODE)
  }

  def newClientBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicy): AMQPBridge = {
    newClientBridge(name, handler,  TRANSIENT_AUTODELETE,policy)
  }

  def newClientBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                      policy: ConnectionSharePolicy): AMQPBridge = {
    new ClientAMQPBridge(name, getConnectionForClientBridge(name, policy))
                        .createAndBindQueues(messageStorePolicy.queueParams)
                        .bindConsumerToQueue(handler)
  }
}

abstract class AMQPBridge(private[sagat] val nodeName: String,
                          private[sagat] val conn: EnhancedConnection) extends Logging
            with ControlStructures {
  require(nodeName != null)
  require(conn != null)

  private[sagat] lazy val inboundExchangeName = "actor.exchange.in." + nodeName
  private[sagat] lazy val outboundExchangeName = "actor.exchange.out." + nodeName
  private[sagat] lazy val inboundQueueName = "actor.queue.in." + nodeName
  private[sagat] lazy val outboundQueueName = "actor.queue.out." + nodeName
  private[sagat] lazy val routingKey_in = "default_in"
  private[sagat] lazy val routingKey_out = "default_out"


  private[sagat] def bindConsumerToQueue(handler: MessageHandler): AMQPBridge
  private[sagat] def createAndBindQueues(params: QueueConfig.QueueParameters): AMQPBridge
  def sendMessage(message: Array[Byte]): Unit

  def shutdown = {
    log.info("Shutting down AMQPBridge for {}", nodeName)
    conn.close
  }
}

private[sagat] class ClientAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name,connection){

  private var targetExchange = inboundExchangeName

  private[sagat] def bindConsumerToQueue(handler: MessageHandler): ClientAMQPBridge = {
    withOpenChannelsOrException(connection.readChannel, connection.writeChannel){
      log.debug("Binding consumer to {}", inboundQueueName)
      val consumer = new BridgeConsumer(this, handler)
      connection.readChannel.basicConsume(inboundQueueName, false, consumer)
      connection.writeChannel.setReturnListener(consumer)
    }
    this
  }

  def sendMessage(message: Array[Byte]): Unit = {
    withOpenChannelsOrException(connection.writeChannel){
      val result = connection.writeChannel.basicPublish(targetExchange, routingKey_out, true, false, null, message)
    }

  }

  private[sagat] def createAndBindQueues(params: QueueConfig.QueueParameters): ClientAMQPBridge = {
    if(params.exclusive){
      targetExchange = outboundExchangeName
      withOpenChannelsOrException(connection.readChannel){
        log.debug("Creating inbound queue {}", Array(inboundQueueName, inboundExchangeName, routingKey_in))
        connection.readChannel.queueDeclare(inboundQueueName,
                                            params.durable,
                                            params.exclusive,
                                            params.autoDelete,
                                            params.arguments)
        log.debug("Binding inbound queue {}", Array(inboundQueueName, inboundExchangeName, routingKey_in))
        connection.readChannel.queueBind(inboundQueueName, inboundExchangeName, routingKey_in)
      }
    }
    this
  }
}

private[sagat] class ServerAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name, connection){

  private[sagat] def createAndBindInboundQueue(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    if(!params.exclusive){
      withOpenChannelsOrException(connection.writeChannel){
        connection.writeChannel.queueDeclare(inboundQueueName,
                           params.durable,
                           params.exclusive,
                           params.autoDelete,
                           params.arguments)
        log.debug("Binding inbound queue {}", Array(inboundQueueName, inboundExchangeName, routingKey_in))
        connection.writeChannel.queueBind(inboundQueueName, inboundExchangeName, routingKey_in)
      }
    }
    this
  }

  private[sagat] def bindConsumerToQueue(handler: MessageHandler): ServerAMQPBridge = {
    withOpenChannelsOrException(connection.readChannel, connection.writeChannel){
      log.debug("Binding consumer to {}", outboundQueueName)
      val consumer = new BridgeConsumer(this, handler)
      connection.readChannel.basicConsume(outboundQueueName, false, consumer)
      connection.writeChannel.setReturnListener(consumer)
    }
    this
  }

  private[sagat] def createExchange(params: ExchangeConfig.ExchangeParameters): ServerAMQPBridge = {
    withOpenChannelsOrException(connection.writeChannel){
      log.debug("Creating Exchange {} ", Array(inboundExchangeName, params))
      connection.writeChannel.exchangeDeclare(inboundExchangeName,
                              params.typeConfig,
                              params.durable,
                              params.autoDelete,
                              params.arguments)
      /* exclusive policy? */
      if(params.fanout){
        log.debug("Creating Exchange {} ", Array(outboundExchangeName, params))
        connection.writeChannel.exchangeDeclare(outboundExchangeName,
                              params.typeConfig,
                              params.durable,
                              params.autoDelete,
                              params.arguments)
      }
    }
    this
  }

  private[sagat] def createAndBindOutboundQueue(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    withOpenChannelsOrException(connection.readChannel){
      log.debug("Creating outbound queue {}", Array(outboundQueueName, params))
      /* declare using the same connection that will bind the listener to enable exclusive consumers */
      connection.readChannel.queueDeclare(outboundQueueName,
                           params.durable,
                           params.exclusive,
                           params.autoDelete,
                           params.arguments)
      val exchangeName = if(params.exclusive) outboundExchangeName else inboundExchangeName
      log.debug("Binding outbound queue {} ", Array(outboundQueueName, exchangeName, routingKey_out))
      connection.readChannel.queueBind(outboundQueueName, exchangeName, routingKey_out)
    }
    this
  }

  private[sagat] def createAndBindQueues(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    createAndBindInboundQueue(params)
    createAndBindOutboundQueue(params)
    this
  }

  def sendMessage(message: Array[Byte]): Unit = {
    withOpenChannelsOrException(connection.writeChannel){
      val result = connection.writeChannel.basicPublish(inboundExchangeName, routingKey_in, true, false, null, message)
    }
  }

}

class BridgeConsumer(bridge: AMQPBridge, handler: MessageHandler) extends DefaultConsumer(bridge.conn.readChannel)
                                                                                      with Logging with ReturnListener{
  require(bridge != null)
  require(handler != null)
  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, message: Array[Byte])  =
    handler.process(message) match {
      case (true,_)         => getChannel.basicAck(envelope.getDeliveryTag, false)
      case (false, requeue) => getChannel.basicReject(envelope.getDeliveryTag, requeue)
    }

  override def handleConsumeOk(consumerTag: String) = {
    log.debug("Registered consumer handler with tag {}", Array(handler.getClass.getName, consumerTag))
  }

  def handleBasicReturn(rejectCode: Int, replyText: String, exchange: String, routingKey: String,
                        properties: BasicProperties, message: Array[Byte]) = {

    // TODO lancar exception
    println("mensagem rejeitada")
    println("%d %s %s %s %s".format(rejectCode, replyText, exchange, routingKey, new String(message)))

  }

  override def handleShutdownSignal(consumerTag: String, signal: ShutdownSignalException){
    if(signal.isHardError){
      /* connection error */
      if(signal.isInitiatedByApplication){
        log.info("Application shutdown of {}",Array(bridge.nodeName,signal))
      }
      else{
        log.error("Forced shutdown of {}",Array(bridge.nodeName, signal))
        /* at least one connection was dropped - force the bridge to shutdown */
        bridge.shutdown
      }
    }else{
      /* channel error */
      /* for now, just drop everything */
      log.error("Shutdown forced by channel dropped {}",Array(bridge.nodeName, signal.toString))
      bridge.shutdown

    }
  }
}