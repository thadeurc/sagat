package akka.remote.amqp

import StorageAndConsumptionPolicy._
import util._
import java.lang.String
import ConnectionSharePolicy._

trait MessageHandler {
  def handleReceived(message: Array[Byte]): Boolean
  def handleRejected(message: Array[Byte], clientId: String): Unit
}

object AMQPBridge extends Logging {

  def newServerBridge(name: String, handler: MessageHandler): ServerAMQPBridge = {
    newServerBridge(name, handler,  EXCLUSIVE_AUTODELETE , ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams): ServerAMQPBridge = {
    newServerBridge(name, handler, messageStorePolicy, ONE_CONN_PER_NODE)
  }

  def newServerBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicyParams): AMQPBridge = {
    newServerBridge(name, handler, EXCLUSIVE_AUTODELETE , policy)
  }

  def newServerBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                      policy: ConnectionSharePolicyParams): ServerAMQPBridge = {
    new ServerAMQPBridge(name, newConnection(name, policy))
      .setup(handler, messageStorePolicy.exchangeParams, messageStorePolicy.queueParams)
  }

  def newClientBridge(name: String, handler: MessageHandler, idSuffix: String): ClientAMQPBridge = {
    newClientBridge(name, handler, ONE_CONN_PER_NODE, idSuffix)
  }

  def newClientBridge(name: String, handler: MessageHandler, policy: ConnectionSharePolicyParams, idSuffix: String): ClientAMQPBridge = {
    newClientBridge(name, handler,  EXCLUSIVE_AUTODELETE , policy, idSuffix)
  }

  def newClientBridge(name: String, handler: MessageHandler, messageStorePolicy: MessageStorageAndConsumptionPolicyParams,
                     policy: ConnectionSharePolicyParams, idSuffix: String): ClientAMQPBridge = {
    new ClientAMQPBridge(name, newConnection(name, policy), idSuffix).setup(handler, messageStorePolicy.queueParams)
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
  private[amqp] lazy val inboundExchangeName = "actor.exchange.in.%s".format(nodeName)
  private[amqp] lazy val inboundQueueName    = "actor.queue.in.%s".format(nodeName)
  private[amqp] lazy val outboundQueueName   = "actor.queue.out.%s".format(nodeName)
  private[amqp] lazy val routingKeyToServer  = "to.server.%s".format(nodeName)

  def sendMessageTo(message: Array[Byte], to: String): Unit

  def shutdown = {
    connection.close
  }
}

class ClientAMQPBridge(name: String, connection: SupervisedConnectionWrapper, idSuffix: String) extends AMQPBridge(name, connection) {
  lazy val id =  "client.%s.%s".format(nodeName, idSuffix)


  def setup(handler: MessageHandler, queueParams: QueueConfig.QueueParameters): ClientAMQPBridge = {
    connection.clientSetup(
      RemoteClientSetup(handler,
        ClientSetupInfo(config = queueParams, name = outboundQueueName + id, exchangeToBind = inboundExchangeName, routingKey = id))
    )
    this
  }

  def sendMessageToServer(message: Array[Byte]): Unit = {
    sendMessageTo(message, routingKeyToServer)
  }

  def sendMessageTo(message: Array[Byte], to: String): Unit = {
    connection.publishTo(exchange = inboundExchangeName, routingKey = to, message)
  }
}

class ServerAMQPBridge(name: String, connection: SupervisedConnectionWrapper) extends AMQPBridge(name, connection){
  private[amqp] lazy val id = "server.%s".format(nodeName)

  def setup(handler: MessageHandler, exchangeParams: ExchangeConfig.ExchangeParameters,
            queueParams: QueueConfig.QueueParameters): ServerAMQPBridge = {
    connection.serverSetup(
      RemoteServerSetup(handler,
        ServerSetupInfo(exchangeParams,
                        queueParams,
                        exchangeName = inboundExchangeName,
                        queueName    = inboundQueueName,
                        routingKey   = routingKeyToServer))
    )
    this
  }

  def sendMessageTo(message: Array[Byte], to: String): Unit = {
    connection.publishTo(inboundExchangeName, to, message)
  }

}
