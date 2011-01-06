package br.ime.usp.sagat.amqp

import StoragePolicy._
import com.rabbitmq.client._
import util.{EnhancedConnection, ControlStructures, Logging}
import util.ConnectionSharePolicy._
import util.ConnectionPool._

/* TODO:
         3- tratamento de erros
         6- usar atores para controle de conexao aberta e como listeners?
         8- pensar em processo de shutdown (deixar configuravel o clean up?)
*/

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

  def disconnect = {
 /*   withOpenChannelOrException(connection.readChannel) {
      connection.readChannel.close
    }
    withOpenChannelOrException(connection.writeChannel) {
      connection.writeChannel.close
    }*/
  }
}

private[sagat] class ClientAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name,connection){

  private[sagat] def bindConsumerToQueue(handler: MessageHandler): ClientAMQPBridge = {
    withOpenChannelOrException(connection.readChannel){
      log.debug("Binding consumer to {}", inboundQueueName)
      connection.readChannel.basicConsume(inboundQueueName, false, new BridgeConsumer(connection.readChannel, handler))
    }
    this
  }

  def sendMessage(message: Array[Byte]): Unit = {
    withOpenChannelOrException(connection.writeChannel){
      val result = connection.writeChannel.basicPublish(exchangeName, routingKey_out, true, true, null, message)
      // TODO fazer algo com o resultado
    }

  }
}

private[sagat] class ServerAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name, connection){

  private[sagat] def bindConsumerToQueue(handler: MessageHandler): ServerAMQPBridge = {
    withOpenChannelOrException(connection.readChannel){
      log.debug("Binding consumer to {}", outboundQueueName)
      connection.readChannel.basicConsume(outboundQueueName, false, new BridgeConsumer(connection.readChannel, handler))
    }
    this
  }

  private[sagat] def createExchange(params: ExchangeConfig.ExchangeParameters): ServerAMQPBridge = {
    withOpenChannelOrException(connection.writeChannel){
      log.debug("Creating Exchange {} ", Array(exchangeName, params))
      connection.writeChannel.exchangeDeclare(exchangeName,
                              params.typeConfig,
                              params.durable,
                              params.autoDelete,
                              params.arguments)
    }
    this
  }

  private[sagat] def createAndBindInboundQueue(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    withOpenChannelOrException(connection.writeChannel){
      log.debug("Creating inbound queue {}", Array(inboundQueueName, params))
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

  private[sagat] def createAndBindOutboundQueue(params: QueueConfig.QueueParameters): ServerAMQPBridge = {
    withOpenChannelOrException(connection.writeChannel){
      log.debug("Creating outbound queue {}", Array(outboundQueueName, params))
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
    withOpenChannelOrException(connection.writeChannel){
      val result = connection.writeChannel.basicPublish(exchangeName, routingKey_in, true, true, null, message)
        // TODO fazer algo com o resultado
    }
  }

}

class BridgeConsumer(channel: Channel, handler: MessageHandler) extends DefaultConsumer(channel) {
  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, message: Array[Byte])  =
    handler.process(message) match {
      case (true,_)         => getChannel.basicAck(envelope.getDeliveryTag, false)
      case (false, requeue) => getChannel.basicReject(envelope.getDeliveryTag, requeue)
    }

  override def handleConsumeOk(consumerTag: String) = {
    /* Called when the consumer is first registered by a call to Channel.basicConsume(java.lang.String, com.rabbitmq.client.Consumer). */
  }

  override def handleShutdownSignal(consumerTag: String, signal: ShutdownSignalException){
    /*  Called to the consumer that either the channel or the undelying connection has been shut down.*/
  }

}