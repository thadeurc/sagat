package br.ime.usp.sagat.amqp

import StorageAndConsumptionPolicy._
import com.rabbitmq.client._
import util.{EnhancedConnection, ControlStructures, Logging}
import util.ConnectionSharePolicy._
import util.ConnectionPool._
import com.rabbitmq.client.AMQP.BasicProperties
import java.lang.String

trait MessageHandler {
  def process(message: Array[Byte], reply: Array[Byte] => Unit): (Boolean, Boolean)
}

object AMQPBridge extends Logging {

  def newServerBridge(name: String, handler: MessageHandler): ServerAMQPBridge = {
    newServerBridge(name, handler,  TRANSIENT_AUTODELETE, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams): ServerAMQPBridge = {
    newServerBridge(name, handler, messageStorePolicy, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicy): AMQPBridge = {
    newServerBridge(name, handler, TRANSIENT_AUTODELETE, policy)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams, policy: ConnectionSharePolicy): ServerAMQPBridge = {
    new ServerAMQPBridge(name, getConnectionForServerBridge(name, policy))
                         .createExchange(messageStorePolicy.exchangeParams)
                         .createAndBindQueue(messageStorePolicy.queueParams, messageStorePolicy.fanout)
                         .bindConsumerToQueue(handler)
  }

  def newClientBridge(name: String, handler: MessageHandler): ClientAMQPBridge = {
    newClientBridge(name, handler, ONE_CONN_PER_NODE)
  }

  def newClientBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicy): ClientAMQPBridge = {
    newClientBridge(name, handler,  TRANSIENT_AUTODELETE,policy)
  }

  def newClientBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                      policy: ConnectionSharePolicy): ClientAMQPBridge = {
    new ClientAMQPBridge(name, getConnectionForClientBridge(name, policy))
                        .createAndBindQueue(messageStorePolicy.queueParams, messageStorePolicy.fanout)
                        .bindConsumerToQueue(handler)
  }
}

abstract class AMQPBridge(private[sagat] val nodeName: String,
                          private[sagat] val conn: EnhancedConnection) extends Logging
            with ControlStructures {

  require(nodeName != null)

  require(conn != null)

  private[sagat] val id: String
  private[sagat] lazy val inboundExchangeName = "actor.exchange.in." + nodeName
  private[sagat] lazy val outboundExchangeName = "actor.exchange.out." + nodeName
  private[sagat] lazy val inboundQueueName = "actor.queue.in." + nodeName
  private[sagat] lazy val outboundQueueName = "actor.queue.out." + nodeName
  private[sagat] lazy val routingKeyToServer = "to.server." + nodeName
  private[sagat] def bindConsumerToQueue(handler: MessageHandler): AMQPBridge
  private[sagat] def createAndBindQueue(params: QueueConfig.QueueParameters, fanout: Boolean): AMQPBridge
  private[sagat] def sendMessageTo(message: Array[Byte], to: Option[String]): Unit

  def shutdown = {
    log.info("Shutting down AMQPBridge for {}", nodeName)
    conn.close
  }
}

private[sagat] class ClientAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name,connection){
  import scala.util.Random._

  private var targetExchange = inboundExchangeName

  private[sagat] lazy val id = {
    // TODO improve ID generation
    "client." + nodeName + "." + nextInt.abs
  }

  private[sagat] def bindConsumerToQueue(handler: MessageHandler): ClientAMQPBridge = {
    withOpenChannelsOrException(connection.readChannel, connection.writeChannel){
      log.debug("Binding consumer to {}", inboundQueueName)
      val consumer = new BridgeConsumer(this, handler)
      connection.readChannel.basicConsume(inboundQueueName, false, consumer)
      connection.writeChannel.setReturnListener(consumer)
    }
    this
  }

  // TODO - vai sumir qdo eu criar um protocolo de wrap, ou alterar o do akka
  private def addId(message: Array[Byte]): Array[Byte] = {
    new String("senderId=" + id + "#" + new String(message)).getBytes
  }

  def sendMessageToServer(message: Array[Byte]): Unit = {
    sendMessageTo(message, Some(routingKeyToServer))
  }

  private[sagat] def sendMessageTo(message: Array[Byte], to: Option[String]): Unit = {
    withOpenChannelsOrException(connection.writeChannel){
      val routingKey = to.getOrElse("fanout")
      connection.writeChannel.basicPublish(targetExchange, routingKey, true, false, null, addId(message))
    }

  }

  private[sagat] def createAndBindQueue(params: QueueConfig.QueueParameters, fanout: Boolean): ClientAMQPBridge = {
    if(fanout){
      targetExchange = outboundExchangeName
    }
    withOpenChannelsOrException(connection.readChannel){
      log.debug("Creating inbound queue {}", Array(inboundQueueName, inboundExchangeName, id))
      connection.readChannel.queueDeclare(inboundQueueName,
                                          params.durable,
                                          params.exclusive,
                                          params.autoDelete,
                                          params.arguments)
      log.debug("Binding inbound queue {}", Array(inboundQueueName, inboundExchangeName, id))
      connection.readChannel.queueBind(inboundQueueName, inboundExchangeName, id)
    }
    this
  }
}

private[sagat] class ServerAMQPBridge(name: String, connection: EnhancedConnection) extends AMQPBridge(name, connection){
  private[sagat] lazy val id = "server." + nodeName

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

  private[sagat] def createAndBindQueue(params: QueueConfig.QueueParameters, fanout: Boolean): ServerAMQPBridge = {
    withOpenChannelsOrException(connection.readChannel){
      log.debug("Creating outbound queue {}", Array(outboundQueueName, params))
      /* declare using the same connection that will bind the listener to enable exclusive consumers */
      connection.readChannel.queueDeclare(outboundQueueName,
                           params.durable,
                           params.exclusive,
                           params.autoDelete,
                           params.arguments)
      val exchangeName = if(fanout) outboundExchangeName else inboundExchangeName
      log.debug("Binding outbound queue {} ", Array(outboundQueueName, exchangeName, routingKeyToServer))
      connection.readChannel.queueBind(outboundQueueName, exchangeName, routingKeyToServer)
    }
    this
  }

  def sendMessageTo(message: Array[Byte], to: Option[String]): Unit = {
    withOpenChannelsOrException(connection.writeChannel){
      val routingKey = to.getOrElse("fanout")
      connection.writeChannel.basicPublish(inboundExchangeName, routingKey, true, false, null, message)
    }
  }

}

class BridgeConsumer(bridge: AMQPBridge, handler: MessageHandler)
   extends DefaultConsumer(bridge.conn.readChannel) with Logging with ReturnListener {
  require(bridge != null)
  require(handler != null)
  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, message: Array[Byte])  = {
    val from = getFrom(message)
    handler.process(message, bridge.sendMessageTo(_,from)) match {
      case (true,_)         => getChannel.basicAck(envelope.getDeliveryTag, false)
      case (false, requeue) => getChannel.basicReject(envelope.getDeliveryTag, requeue)
    }
  }

  private def getFrom(msg: Array[Byte]): Option[String] = {
    val text = new String(msg);

    if(text.indexOf("senderId=") >= 0){
      Some(text.replace("senderId=", "").split("#")(0))
    }
    else None
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
        log.info("Application shutdown of {}",Array(bridge.id, signal))
      }
      else{
        log.error("Forced shutdown of {}",Array(bridge.id, signal))
        /* at least one connection was dropped - force the bridge to shutdown */
        bridge.shutdown
      }
    }else{
      /* channel error */
      /* for now, just drop everything */
      log.error("Shutdown forced by channel dropped {}",Array(bridge.id, signal.toString))
      bridge.shutdown

    }
  }
}