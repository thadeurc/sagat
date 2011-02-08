package br.ime.usp.sagat.amqp.util

import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy._
import com.rabbitmq.client._
import java.io.IOException
import akka.actor.{ActorRef, Exit, Actor}
import akka.config.Supervision.{Permanent, OneForOneStrategy }
import br.ime.usp.sagat.amqp.{ExchangeConfig, QueueConfig, MessageHandler}

abstract class ConnectionFactoryWithLimitedChannels(policy: ConnectionSharePolicyParams) {
  require(policy != null)
  lazy val factory: ConnectionFactory = {
    val cf = new ConnectionFactory
    cf.setHost("localhost")
    cf.setUsername("actor_admin")
    cf.setPassword("actor_admin")
    cf.setVirtualHost("/actor_host")
    cf.setRequestedChannelMax(policy.channels)
    cf.setRequestedHeartbeat(15) /* to confirm the network is ok - value in seconds */
    cf
  }
  def newConnection: Connection = factory.newConnection
}
class ReadAndWriteConnectionFactory extends ConnectionFactoryWithLimitedChannels(ONE_CONN_PER_NODE)
class ReadOrWriteConnectionFactory extends ConnectionFactoryWithLimitedChannels(ONE_CONN_PER_CHANNEL)

private case object Connect
private case class ConnectionShutdown(cause: ShutdownSignalException)
private case object WriteChannelRequest
private case object ReadChannelRequest
private case object StartWriteChannel
private case object StartReadChannel
private case object NeedToRequestNewChannel
private case object ReconnectRemoteClientSetup
private case object ReconnectRemoteServerSetup
case class RemoteClientSetup(handler: MessageHandler, config: ClientSetupInfo){
  require(handler != null)
  require(config  != null)
}
case class ClientSetupInfo(config: QueueConfig.QueueParameters, name: String, exchangeToBind: String, routingKey: String){
  require(config != null)
  require(name   != null)
  require(exchangeToBind != null)
  require(routingKey != null)
}
case class BasicPublish(exchange: String, routingKey: String, mandatory: Boolean, immediate: Boolean, message: Array[Byte]){
  require(exchange != null)
  require(routingKey != null)
  require(message != null)
}

case class RemoteServerSetup(handler: MessageHandler, config: ServerSetupInfo){
  require(handler != null)
  require(config  != null)
}
case class ServerSetupInfo(exchangeConfig: ExchangeConfig.ExchangeParameters,
                           queueConfig: QueueConfig.QueueParameters,
                           exchangeName: String, queueName: String, routingKey: String, fanoutExchangeName: Option[String]){
  require(exchangeConfig != null)
  require(queueConfig != null)
  require(exchangeName   != null)
  require(queueName != null)
  require(routingKey != null)
  require(fanoutExchangeName != null)
}


trait AMQPSupervisor {
  import Actor._
  private val supervisor = actorOf(new AMQPConnectionFactory).start
  private[util] val readOrWriteConnFactory  =  new ReadOrWriteConnectionFactory
  private[util] val readAndWriteConnFactory =  new ReadAndWriteConnectionFactory

  def shutdownAll = {
    supervisor.shutdownLinkedActors
  }

  def newSupervisedConnection(nodeName: String, policy: ConnectionSharePolicyParams): ActorRef = {
    val connectionActor = actorOf(new ConnectionActor(nodeName, policy))
    supervisor.startLink(connectionActor)
    connectionActor ! Connect
    connectionActor
  }

  def newSupervisedReadChannel(supervisor: ActorRef): ActorRef = {
    val channel = actorOf(new ReadChannelActor)
    supervisor.startLink(channel)
    channel ! StartReadChannel
    channel
  }

  def newSupervisedWriteChannel(supervisor: ActorRef): ActorRef = {
    val channel = actorOf(new WriteChannelActor)
    supervisor.startLink(channel)
    channel ! StartWriteChannel
    channel
  }

  def createNewFaultTolerantConnection(nodeName: String, policy: ConnectionSharePolicyParams): FaultTolerantConnection = {
    val conn = newSupervisedConnection(nodeName, policy)
    val read = newSupervisedReadChannel(conn)
    val write = newSupervisedWriteChannel(conn)
    new FaultTolerantConnection(readChannel = read, writeChannel = write)
  }
}

object AMQPConnectionFactory extends AMQPSupervisor

class AMQPConnectionFactory extends Actor {
  self.id = "amqp.supervisor"
  self.lifeCycle = Permanent
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 2000)
  def shutdownAll = {
    self.shutdownLinkedActors
  }

  def receive = {
    case _ => {}
  }

}

class ConnectionActor(myId: String, val policy: ConnectionSharePolicyParams) extends Actor {
  import AMQPConnectionFactory._
  self.id = myId
  self.lifeCycle = Permanent
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 2000)
  private var readConn:  Option[Connection] = None
  private var writeConn: Option[Connection] = None

  private def disconnect = {
    try {
      readConn.foreach(_.close)
      policy match {
        case ONE_CONN_PER_CHANNEL => {
          writeConn.foreach(_.close)
        }
      }
    } catch {
      case e: IOException => log.error("Could not close AMQP connection [%s]", self.id)
    }
    readConn = None
    writeConn = None
  }

  private def connect = {
    log.info("Connecting [%s] with [%s] policy", self.id, policy)
    policy match {
      case ONE_CONN_PER_NODE => {
        try {
          if (readConn.isEmpty || !readConn.get.isOpen) {
            readConn = Some(readAndWriteConnFactory.newConnection)
            writeConn = readConn
            readConn.foreach{
              conn => conn.addShutdownListener(new ShutdownListener {
                def shutdownCompleted(cause: ShutdownSignalException) {
                  self ! ConnectionShutdown(cause)
                }
              })
            }
          }
        }catch{
          case io: IOException => {
            log.error("Could not connect [%s] to AMQP broker [%s]", self.id, readAndWriteConnFactory.toString)
            throw io
          }
        }
      }
      case ONE_CONN_PER_CHANNEL => {
        try {
          if (readConn.isEmpty || !readConn.get.isOpen) {
            readConn = Some(readOrWriteConnFactory.newConnection)
            readConn.foreach{
              conn => conn.addShutdownListener(new ShutdownListener {
                def shutdownCompleted(cause: ShutdownSignalException) {
                  self ! ConnectionShutdown(cause)
                }
              })
            }
          }
          if (writeConn.isEmpty || !writeConn.get.isOpen) {
            writeConn = Some(readOrWriteConnFactory.newConnection)
            writeConn.foreach{
              conn => conn.addShutdownListener(new ShutdownListener {
                def shutdownCompleted(cause: ShutdownSignalException) {
                  self ! ConnectionShutdown(cause)
                }
              })
            }
          }
        }catch {
          case io: IOException => log.error("Could not connect [%s] to AMQP broker [%s]", self.id, readOrWriteConnFactory.toString)
          throw io
        }
      }
    }
    log.info("Successfully (re)connected [%s]", self.id)
    log.debug("Sending NeedToRequestNewChannel to %d already linked actors", self.linkedActors.size)
    import scala.collection.JavaConversions._
    self.linkedActors.values.iterator.foreach(_ ! NeedToRequestNewChannel)
  }

  private def requestReadChannel: Unit = {
    readConn match {
      case Some(conn) => self.reply(Some(conn.createChannel))
      case _ =>
        log.warning("Unable to create new read channel - no read connection")
        self.reply(None)
    }
  }

  private def requestWriteChannel: Unit = {
    writeConn match {
      case Some(conn) => self.reply(Some(conn.createChannel))
      case _ =>
        log.warning("Unable to create new write channel - no write connection")
        self.reply(None)
    }
  }

  private def shutdownReceived(cause: ShutdownSignalException): Unit = {
    if (cause.isHardError) {
      if (cause.isInitiatedByApplication) {
        log.info("ConnectionShutdown by application [%s]", self.id)
      } else {
        log.error(cause, "ConnectionShutdown is hard error - self terminating")
        self ! new Exit(self, cause)
      }
    }
  }

  def receive = {
    case Connect => connect
    case ConnectionShutdown(cause) => shutdownReceived(cause)
    case ReadChannelRequest  => requestReadChannel
    case WriteChannelRequest => requestWriteChannel
  }

  override def postStop = {
    self.shutdownLinkedActors
    disconnect
  }

  def preRestart = {
    disconnect
  }

  def postRestart = {
    self ! Connect
  }
}

trait ChannelActor extends Actor {
  private[util] var channel: Option[Channel] = None
}

class WriteChannelActor extends ChannelActor {
  private var myReturnHandler: Option[MessageHandler] = None
  private var myServerReturnHandler: Option[MessageHandler] = None
  private var myServerConfigs: Option[ServerSetupInfo] = None

  def receive = {
    case StartWriteChannel => {
      if(!channel.isDefined || !channel.get.isOpen){
        self.supervisor.foreach {
          sup =>
            channel = (sup !! WriteChannelRequest).asInstanceOf[Option[Channel]]
          }
      }
    }
    case NeedToRequestNewChannel => {
        self ! StartWriteChannel
    }
    case BasicPublish(exchange, routingKey, mandatory, immediate, message) => {
      channel.foreach {
        ch => ch.basicPublish(exchange, routingKey, mandatory, immediate, null, message)
      }
    }
    case RemoteClientSetup(handler, _) => {
      if(!myReturnHandler.isDefined){
        myReturnHandler = Some(handler)
      }
      channel.foreach {
        ch => ch.setReturnListener(new ReturnListener {
        override def handleBasicReturn(rejectCode: Int, replyText: String, exchange: String, routingKey: String,
                        properties: AMQP.BasicProperties, message: Array[Byte]) {
            /* TODO melhorar esse callback. acho que vou precisar de outras informacoes
            println("mensagem rejeitada")
            println("%d %s %s %s %s".format(rejectCode, replyText, exchange, routingKey, new String(message))) */
            myReturnHandler.get.handleRejectedMessage(message, routingKey)
          }
        })
      }

    }
    case RemoteServerSetup(handler, configs) => {
      if(!myServerReturnHandler.isDefined){
        myServerReturnHandler = Some(handler)
      }
      if(!myServerConfigs.isDefined){
        myServerConfigs = Some(configs)
      }
      setupServer
    }
    /* TODO may need to reconnect and recreate exchanges */
  }

  def setupServer = {
    val config = myServerConfigs.get
    val inboundExchangeName = config.exchangeName
    val outboundExchangeName = config.fanoutExchangeName
    val params = config.exchangeConfig
    channel.foreach{
      ch => {
        log.debug("Creating Exchange {} ", Array(inboundExchangeName, params))
        ch.exchangeDeclare(inboundExchangeName,
                                   params.typeConfig,
                                   params.durable,
                                   params.autoDelete,
                                   params.arguments)
        if(outboundExchangeName.isDefined){
          log.debug("Creating Exchange {} ", Array(outboundExchangeName.get, params))
          ch.exchangeDeclare(outboundExchangeName.get,
                                params.typeConfig,
                                params.durable,
                                params.autoDelete,
                                params.arguments)
        }
        ch.setReturnListener(new ReturnListener {
          override def handleBasicReturn(rejectCode: Int, replyText: String, exchange: String, routingKey: String,
                        properties: AMQP.BasicProperties, message: Array[Byte]){
            /* TODO melhorar esse callback. acho que vou precisar de outras informacoes
            println("mensagem rejeitada")
            println("%d %s %s %s %s".format(rejectCode, replyText, exchange, routingKey, new String(message))) */
            myServerReturnHandler.get.handleRejectedMessage(message, routingKey)
          }
        })
      }
    }
  }
}

class ReadChannelActor extends ChannelActor {
  private var myHandler: Option[MessageHandler] = None
  private var myConfigs: Option[ClientSetupInfo] = None
  private var myServerConfigs: Option[ServerSetupInfo] = None
  private var myServerHandler: Option[MessageHandler] = None

  def receive = {
    case StartReadChannel => {
      if(!channel.isDefined || !channel.get.isOpen){
        self.supervisor.foreach {
          sup =>
            channel = (sup !! ReadChannelRequest).asInstanceOf[Option[Channel]]
        }
      }
    }
    case NeedToRequestNewChannel => {
      self ! StartReadChannel
      self ! ReconnectRemoteClientSetup
    }
    case RemoteClientSetup(handler, configs) => {
      if(!myHandler.isDefined) {
        myHandler = Some(handler)
      }
      if(!myConfigs.isDefined){
        myConfigs = Some(configs)
      }
      clientSetup
    }
    case ReconnectRemoteClientSetup => {
      clientSetup
    }
    case RemoteServerSetup(handler, configs) =>{
      if(!myServerHandler.isDefined) {
        myServerHandler = Some(handler)
      }
      if(!myServerConfigs.isDefined){
        myServerConfigs = Some(configs)
      }
      serverSetup
    }
    case ReconnectRemoteServerSetup => {
      serverSetup
    }

  }

  private def serverSetup = {
    val config = myServerConfigs.get
    val outboundQueueName = config.queueName
    val inboundExchangeName = config.exchangeName
    val params = config.queueConfig
    val handler = myServerHandler.get
    val routingKeyToServer = config.routingKey
    channel.foreach{
      ch => {
        log.debug("Creating outbound queue {}", Array(outboundQueueName, params))
        ch.queueDeclare(outboundQueueName,
                           params.durable,
                           params.exclusive,
                           params.autoDelete,
                           params.arguments)
        val exchangeName = if(config.fanoutExchangeName.isDefined) config.fanoutExchangeName.get else inboundExchangeName
        log.debug("Binding outbound queue {} ", Array(outboundQueueName, exchangeName, routingKeyToServer))
        ch.queueBind(outboundQueueName, exchangeName, routingKeyToServer)
        log.debug("Binding consumer to {}", outboundQueueName)
        val consumer = new BridgeConsumer(ch, handler)
        ch.basicConsume(outboundQueueName, false, consumer)
      }
    }
  }

  private def clientSetup = {
    val config = myConfigs.get
    val inboundQueueName    = config.name
    val inboundExchangeName = config.exchangeToBind
    val id = config.routingKey
    val params = config.config
    val handler = myHandler.get
    channel.foreach{
      ch => {
        log.debug("Creating inbound queue {}", Array(inboundQueueName, inboundExchangeName, id))
        ch.queueDeclare(inboundQueueName,
                        params.durable,
                        params.exclusive,
                        params.autoDelete,
                        params.arguments)
        log.debug("Binding inbound queue {}", Array(inboundQueueName, inboundExchangeName, id))
        ch.queueBind(inboundQueueName, inboundExchangeName, id)
        log.debug("Binding consumer to {}", inboundQueueName)
        val consumer = new BridgeConsumer(ch, handler)
        ch.basicConsume(inboundQueueName, false, consumer)
      }
    }
  }
}

class FaultTolerantConnection(readChannel: ActorRef, writeChannel: ActorRef){

  def close = {
    // TODO implementar
  }

  def clientSetup(setupInfo: RemoteClientSetup) {
    readChannel  ! RemoteClientSetup
    writeChannel ! RemoteClientSetup
  }

  def serverSetup(setupInfo: RemoteServerSetup) {
    writeChannel ! RemoteServerSetup
    readChannel  ! RemoteServerSetup
  }

  def publishTo(exchange: String, routingKey: String, message: Array[Byte]) {
    writeChannel ! BasicPublish(exchange, routingKey, mandatory = true, immediate = false, message)
  }
}

class BridgeConsumer(channel: Channel, handler: MessageHandler) extends DefaultConsumer(channel) with Logging{
  require(channel != null && channel.isOpen)
  require(handler != null)
  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, message: Array[Byte])  = {
    if(handler.handleMessageReceived(message)){
      channel.basicAck(envelope.getDeliveryTag, false)
    }
  }

  override def handleConsumeOk(consumerTag: String) = {
    log.debug("Registered consumer handler with tag {}", Array(handler.getClass.getName, consumerTag))
  }


}
