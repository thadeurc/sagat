package br.ime.usp.sagat.amqp

import StorageMode._
import com.rabbitmq.client._
import util.{EnhancedConnection, ControlStructures, Logging}
import util.ConnectionSharePolicy._
import util.ConnectionPool._

/* TODO:
         3- tratamento de erros
         6- usar atores para controle de conexao aberta e como listeners?
         7- pensar no que fazer na confirmacao de recebimento das mensagens
         8- pensar em processo de shutdown (deixar configuravel o clean up?)
*/


object AMQPBridge extends Logging {

  def newServerBridge(name: String, consumerListener: Consumer): AMQPBridge = {
    newServerBridge(name, consumerListener, TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, consumerListener: Consumer,
                          messageStoreMode: MessageStoreModeParams): AMQPBridge = {
    newServerBridge(name, consumerListener, messageStoreMode, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, consumerListener: Consumer, policy: ConnectionSharePolicy): AMQPBridge = {
    newServerBridge(name, consumerListener, TRANSIENT_AUTODELETE, policy)
  }


  def newServerBridge(name: String, consumerListener: Consumer,
                          messageStoreMode: MessageStoreModeParams, policy: ConnectionSharePolicy): AMQPBridge = {
    new ServerAMQPBridge(name, getConnectionForServerBridge(name, policy))
                         .createExchange(messageStoreMode.exchangeParams)
                         .createAndBindQueues(messageStoreMode.queueParams)
                         .bindConsumerToQueue(consumerListener)
  }


  def newClientBridge(name: String, consumerListener: Consumer): AMQPBridge = {
    newClientBridge(name, consumerListener, ONE_CONN_PER_NODE)
  }

  def newClientBridge(name: String, consumerListener: Consumer, policy: ConnectionSharePolicy): AMQPBridge = {
    new ClientAMQPBridge(name, getConnectionForClientBridge(name, policy)).bindConsumerToQueue(consumerListener)
  }

}

abstract class AMQPBridge(private[sagat] val nodeName: String, private[sagat] val conn: EnhancedConnection) extends Logging with ControlStructures {
  require(nodeName != null)

  require(conn != null)

  private[sagat] lazy val exchangeName = "actor.exchange." + nodeName

  private[sagat] lazy val inboundQueueName = "actor.queue.in." + nodeName

  private[sagat] lazy val outboundQueueName = "actor.queue.out." + nodeName

  private[sagat] lazy val routingKey_in = "default_in"

  private[sagat] lazy val routingKey_out = "default_out"

  private[sagat] def bindConsumerToQueue(consumerListener: Consumer): AMQPBridge

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

  private[sagat] def bindConsumerToQueue(consumerListener: Consumer): ClientAMQPBridge = {
    require(consumerListener != null)
    withOpenChannelOrException(connection.readChannel){
      log.debug("Binding consumer to {}", inboundQueueName)
      connection.readChannel.basicConsume(inboundQueueName, true, consumerListener)
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

  private[sagat] def bindConsumerToQueue(consumerListener: Consumer): ServerAMQPBridge = {
    require(consumerListener != null)
    withOpenChannelOrException(connection.readChannel){
      log.debug("Binding consumer to {}", outboundQueueName)
      connection.readChannel.basicConsume(outboundQueueName, true, consumerListener)
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