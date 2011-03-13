package akka.remote.amqp

import StorageAndConsumptionPolicy._
import util._
import java.lang.String
import ConnectionSharePolicy._

trait MessageHandler {
  def handleMessageReceived(message: Array[Byte]): Boolean
  def handleRejectedMessage(message: Array[Byte], clientId: String): Unit
}

object AMQPBridge extends Logging {

  def newServerBridge(name: String, handler: MessageHandler): ServerAMQPBridge = {
    newServerBridge(name, handler,  EXCLUSIVE_TRANSIENT, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams): ServerAMQPBridge = {
    newServerBridge(name, handler, messageStorePolicy, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicyParams): AMQPBridge = {
    newServerBridge(name, handler, EXCLUSIVE_TRANSIENT, policy)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                      policy: ConnectionSharePolicyParams): ServerAMQPBridge = {
    new ServerAMQPBridge(name, newConnection(name, policy))
      .setup(handler, messageStorePolicy.exchangeParams, messageStorePolicy.queueParams, messageStorePolicy.exchangeParams.fanout)
  }

  def newClientBridge(name: String, handler: MessageHandler): ClientAMQPBridge = {
    newClientBridge(name, handler, ONE_CONN_PER_NODE)
  }

  def newClientBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicyParams): ClientAMQPBridge = {
    newClientBridge(name, handler,  EXCLUSIVE_TRANSIENT, policy)
  }

  def newClientBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                     policy: ConnectionSharePolicyParams): ClientAMQPBridge = {
    new ClientAMQPBridge(name, newConnection(name, policy)).setup(handler, messageStorePolicy.queueParams, messageStorePolicy.fanout)
  }

  def newConnection(name: String, policy: ConnectionSharePolicyParams): SupervisedConnectionWrapper = {
    AMQPConnectionFactory.createNewSupervisedConnection(name, policy)
  }

}

abstract class AMQPBridge(val nodeName: String,
                          val connection: SupervisedConnectionWrapper) extends Logging {

  require(nodeName != null)
  require(connection != null)
  private[amqp] val id: String
  private[amqp] lazy val inboundExchangeName = "actor.exchange.in." + nodeName
  private[amqp] lazy val outboundExchangeName = "actor.exchange.out." + nodeName
  private[amqp] lazy val inboundQueueName = "actor.queue.in."+ nodeName
  private[amqp] lazy val outboundQueueName = "actor.queue.out."
  private[amqp] lazy val routingKeyToServer = "to.server." + nodeName
  def sendMessageTo(message: Array[Byte], to: Option[String]): Unit
  def shutdown = {
    connection.close
  }
}

class ClientAMQPBridge(name: String, connection: SupervisedConnectionWrapper) extends AMQPBridge(name, connection) {
  import scala.util.Random._

  private var targetExchange = inboundExchangeName

  lazy val id = {
    "client.%s.%d.%d".format(nodeName, nextInt.abs, nextInt.abs)
  }

  def setup(handler: MessageHandler, queueParams: QueueConfig.QueueParameters, fanout: Boolean): ClientAMQPBridge = {
    if(fanout){
      targetExchange = outboundExchangeName
    }
    connection.clientSetup(
      RemoteClientSetup(handler,
        ClientSetupInfo(config = queueParams, name = outboundQueueName + id, exchangeToBind = inboundExchangeName, routingKey = id))
    )
    this
  }

  def sendMessageToServer(message: Array[Byte]): Unit = {
    sendMessageTo(message, Some(routingKeyToServer))
  }

  def sendMessageTo(message: Array[Byte], to: Option[String]): Unit = {
    connection.publishTo(exchange = targetExchange, routingKey = to.getOrElse("fanout"), message)
  }
}

class ServerAMQPBridge(name: String, connection: SupervisedConnectionWrapper) extends AMQPBridge(name, connection){
  private[amqp] lazy val id = "server." + nodeName

  def setup(handler: MessageHandler, exchangeParams: ExchangeConfig.ExchangeParameters,
            queueParams: QueueConfig.QueueParameters, fanout: Boolean): ServerAMQPBridge = {
    connection.serverSetup(
      RemoteServerSetup(handler,
        ServerSetupInfo(exchangeParams,
                        queueParams,
                        exchangeName = inboundExchangeName,
                        queueName = inboundQueueName,
                        routingKey = routingKeyToServer,
                        if(fanout) Some(outboundExchangeName) else None))
    )
    this
  }

  def sendMessageTo(message: Array[Byte], to: Option[String]): Unit = {
    connection.publishTo(inboundExchangeName, to.getOrElse("fanout"), message)
  }

}
