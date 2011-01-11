package br.ime.usp.sagat.amqp

import StoragePolicy._
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

  def newServerBridge(name: String, handler: MessageHandler, messageStoreMode: MessageStorePolicyParams): AMQPBridge = {
    newServerBridge(name, handler, messageStoreMode, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicy): AMQPBridge = {
    newServerBridge(name, handler, TRANSIENT_AUTODELETE, policy)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStoreMode: MessageStorePolicyParams, policy: ConnectionSharePolicy): AMQPBridge = {
    new ServerAMQPBridge(name, getConnectionForServerBridge(name, policy))
                         .createExchange(messageStoreMode.exchangeParams)
                         .createAndBindQueues(messageStoreMode.queueParams)
                         .bindConsumerToQueue(handler)
  }

  def newClientBridge(name: String, handler: MessageHandler): AMQPBridge = {
    newClientBridge(name, handler, ONE_CONN_PER_NODE)
  }

  def newClientBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicy): AMQPBridge = {
    new ClientAMQPBridge(name, getConnectionForClientBridge(name, policy)).bindConsumerToQueue(handler)
  }

}

abstract class AMQPBridge(private[sagat] val nodeName: String,
                          private[sagat] val conn: EnhancedConnection) extends Logging
            with ControlStructures {
  require(nodeName != null)

  require(conn != null)

  private[sagat] lazy val exchangeName = "actor.exchange." + nodeName

  private[sagat] lazy val inboundQueueName = "actor.queue.in." + nodeName

  private[sagat] lazy val outboundQueueName = "actor.queue.out." + nodeName

  private[sagat] lazy val routingKey_in = "default_in"

  private[sagat] lazy val routingKey_out = "default_out"


  private[sagat] def bindConsumerToQueue(handler: MessageHandler): AMQPBridge

  def sendMessage(message: Array[Byte]): Unit

  def shutdown = {
    log.info("Shutting down AMQPBridge for {}", nodeName)
    conn.close
  }
}

private[sagat] class ClientAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name,connection){

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
      val result = connection.writeChannel.basicPublish(exchangeName, routingKey_out, true, true, null, message)
      // TODO fazer algo com o resultado
    }

  }
}

private[sagat] class ServerAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name, connection){

  private[sagat] def createAndBindInboundQueue(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    withOpenChannelsOrException(connection.writeChannel){
      log.debug("Creating inbound queue {}", Array(inboundQueueName, params))
      connection.writeChannel.queueDelete(inboundQueueName)
      connection.writeChannel.queueDeclare(inboundQueueName,
                           params.durable,
                           params.exclusive,
                           params.autoDelete,
                           params.arguments)
      log.debug("Binging inbound queue {}", Array(inboundQueueName, exchangeName, routingKey_in))
      connection.writeChannel.queueBind(inboundQueueName, exchangeName, routingKey_in)
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
      log.debug("Creating Exchange {} ", Array(exchangeName, params))
      connection.writeChannel.exchangeDelete(exchangeName)
      connection.writeChannel.exchangeDeclare(exchangeName,
                              params.typeConfig,
                              params.durable,
                              params.autoDelete,
                              params.arguments)
    }
    this
  }



  private[sagat] def createAndBindOutboundQueue(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    withOpenChannelsOrException(connection.writeChannel){
      log.debug("Creating outbound queue {}", Array(outboundQueueName, params))
      connection.writeChannel.queueDelete(outboundQueueName)
      connection.writeChannel.queueDeclare(outboundQueueName,
                           params.durable,
                           params.exclusive,
                           params.autoDelete,
                           params.arguments)
      log.debug("Binding outbound queue {} ", Array(outboundQueueName, exchangeName, routingKey_out))
      connection.writeChannel.queueBind(outboundQueueName, exchangeName, routingKey_out)
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
      val result = connection.writeChannel.basicPublish(exchangeName, routingKey_in, true, true, null, message)
        // TODO fazer algo com o resultado
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

  def handleBasicReturn(rejectCode: Int, replyText: String, exchange: String, routingKey: String, properties: BasicProperties, message: Array[Byte]) = {
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