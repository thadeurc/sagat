package br.ime.usp.sagat.amqp.util


import com.rabbitmq.client._
import akka.actor.{ActorRef, Exit, Actor}
import akka.config.Supervision.{Permanent, OneForOneStrategy }
import br.ime.usp.sagat.amqp.{ExchangeConfig, QueueConfig, MessageHandler}
import java.io.IOException

object ConnectionSharePolicy extends Enumeration {
    val ONE_CONN_PER_CHANNEL = Value("ONE_CONN_PER_CHANNEL", channels = 1)
    val ONE_CONN_PER_NODE   =  Value("ONE_CONN_PER_NODE"   , channels = 2)

    class ConnectionSharePolicyParams(name: String, val channels: Int) extends Val(nextId, name)
    protected final def Value(name: String, channels: Int): ConnectionSharePolicyParams = new ConnectionSharePolicyParams(name, channels)
}

import ConnectionSharePolicy._


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

private[amqp] case object Connect
private[amqp] case class ConnectionShutdown(cause: ShutdownSignalException)
private[amqp] case object WriteChannelRequest
private[amqp] case object ReadChannelRequest
private[amqp] case object StartWriteChannel
private[amqp] case object StartReadChannel
private[amqp] case object NeedToRequestNewChannel
private[amqp] case object ReconnectRemoteClientSetup
private[amqp] case object ReconnectRemoteServerSetup
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

  def linkedCount: Int = {
    supervisor.linkedActors.size
  }

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
    new FaultTolerantConnection(connection = conn, readChannel = read, writeChannel = write)
  }
}

object AMQPConnectionFactory extends AMQPSupervisor

class AMQPConnectionFactory extends Actor {
  self.id = "amqp.supervisor"
  self.lifeCycle = Permanent
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 2000)

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
      log.info("Disconnecting connection(s) of actor [%s]", self.id)
      readConn.foreach(_.close)
      policy match {
        case ONE_CONN_PER_CHANNEL => {
          writeConn.foreach(_.close)
        }
        case _ => // does nothing
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
    log.debug("Connection received a read channel request")
    readConn match {
      case Some(conn) => self.reply(conn.createChannel)
      case _ =>
        log.warning("Unable to create new read channel - no read connection")
        self.reply(None)
    }
  }

  private def requestWriteChannel: Unit = {
    log.debug("Connection received a write channel request")
    writeConn match {
      case Some(conn) => self.reply(conn.createChannel)
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
    case unknown => log.warn("ConnectionActor [%s] received unknown message %s", unknown)
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
      log.debug("Starting write channel for actor %s", self.id)
      if(!channel.isDefined || !channel.get.isOpen){
        self.supervisor.foreach {
          sup =>
            val result = (sup !! WriteChannelRequest).asInstanceOf[Option[Channel]]
            result match {
              case Some(value: Channel) => channel = result
              case _ => {
                log.warn("Write channel not received")
                throw new IllegalArgumentException("Write channel did not receive a channel.")
              }
            }

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
      log.debug("Remote client setup received for WriteChannelActor")
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
      log.debug("Remote server setup received for WriteChannelActor")
      if(!myServerReturnHandler.isDefined){
        myServerReturnHandler = Some(handler)
      }
      if(!myServerConfigs.isDefined){
        myServerConfigs = Some(configs)
      }
      setupServer
    }
    case unknown => {
      log.warn("received unknown message in WriteChannel actor %s", unknown)
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
        log.debug("Creating Exchange %s %s", inboundExchangeName, params)
        ch.exchangeDeclare(inboundExchangeName,
                                   params.typeConfig,
                                   params.durable,
                                   params.autoDelete,
                                   params.arguments)
        if(outboundExchangeName.isDefined){
          log.debug("Creating Exchange %s %s",outboundExchangeName.get, params)
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
      log.debug("Starting read channel for actor %s", self.id)
      if(!channel.isDefined || !channel.get.isOpen){
        self.supervisor.foreach {
          sup =>
            val result = (sup !! ReadChannelRequest).asInstanceOf[Option[Channel]]
            result match {
              case Some(value: Channel) => channel = result
              case _ => {
                log.warn("Read channel not received")
                throw new IllegalArgumentException("Read channel did not receive a channel.")
              }
            }

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
      log.debug("Remote server setup received for ReadChannelActor")
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
    case unknown => {
      log.warn("received unknown message in ReadChannel actor %s", unknown)
    }
  }

  private def serverSetup = {
    myServerConfigs.foreach{ config =>
      val outboundQueueName = config.queueName
      val inboundExchangeName = config.exchangeName
      val params = config.queueConfig
      val handler = myServerHandler.get
      val routingKeyToServer = config.routingKey
      channel.foreach{
        ch => {
          log.debug("Creating outbound queue %s %s", outboundQueueName, params)
          ch.queueDeclare(outboundQueueName,
                             params.durable,
                             params.exclusive,
                             params.autoDelete,
                             params.arguments)
          val exchangeName = if(config.fanoutExchangeName.isDefined) config.fanoutExchangeName.get else inboundExchangeName
          log.debug("Binding outbound queue %s %s %s", outboundQueueName, exchangeName, routingKeyToServer)
          ch.queueBind(outboundQueueName, exchangeName, routingKeyToServer)
          log.debug("Binding consumer to %s", outboundQueueName)
          val consumer = new BridgeConsumer(ch, handler)
          ch.basicConsume(outboundQueueName, false, consumer)
        }
      }
    }
  }

  private def clientSetup = {
    log.debug("Remote client setup received for ReadChannelActor")
    myConfigs.foreach { config =>
      val inboundQueueName    = config.name
      val inboundExchangeName = config.exchangeToBind
      val id = config.routingKey
      val params = config.config
      val handler = myHandler.get
      channel.foreach{
        ch => {
          log.debug("Creating inbound queue %s", inboundQueueName, inboundExchangeName, id)
          ch.queueDeclare(inboundQueueName,
                          params.durable,
                          params.exclusive,
                          params.autoDelete,
                          params.arguments)
          log.debug("Binding inbound queue %s %s %s", inboundQueueName, inboundExchangeName, id)
          ch.queueBind(inboundQueueName, inboundExchangeName, id)
          log.debug("Binding consumer to %s", inboundQueueName)
          val consumer = new BridgeConsumer(ch, handler)
          ch.basicConsume(inboundQueueName, false, consumer)
        }
      }
    }
  }
}

class FaultTolerantConnection(connection: ActorRef, readChannel: ActorRef, writeChannel: ActorRef) extends ControlStructures{
  @volatile private[this] var open = true

  def close =
    ifTrueOrException(open){
      open = false
      connection.stop
    }


  def clientSetup(setupInfo: RemoteClientSetup) {
    ifTrueOrException(open){
      readChannel  ! setupInfo
      writeChannel ! setupInfo
    }
  }

  def serverSetup(setupInfo: RemoteServerSetup) {
    ifTrueOrException(open){
      writeChannel ! setupInfo
      readChannel  ! setupInfo
    }
  }

  def publishTo(exchange: String, routingKey: String, message: Array[Byte]) {
    ifTrueOrException(open){
      writeChannel ! BasicPublish(exchange, routingKey, mandatory = true, immediate = false, message)
    }
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
    log.debug("Registered consumer")
  }


}
