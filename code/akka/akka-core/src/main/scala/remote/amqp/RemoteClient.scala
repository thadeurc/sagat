/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.remote.amqp

import se.scalablesolutions.akka.remote.protobuf.RemoteProtocol.{RemoteRequest, RemoteReply}
import se.scalablesolutions.akka.actor.{Exit, Actor}
import se.scalablesolutions.akka.dispatch.{DefaultCompletableFuture, CompletableFuture}
import se.scalablesolutions.akka.util.{UUID, Logging}
import se.scalablesolutions.akka.config.Config.config

import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.compression.{ZlibDecoder, ZlibEncoder}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import org.jboss.netty.util.{TimerTask, Timeout, HashedWheelTimer}

import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.{TimeUnit, Executors, ConcurrentMap, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{HashSet, HashMap}
import se.scalablesolutions.akka.remote.protobuf.RemoteProtocol.RemoteRequest
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope, ShutdownSignalException, DefaultConsumer, Channel => AMQPChannel}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object RemoteRequestIdFactory {
  private val nodeId = UUID.newUuid
  private val id = new AtomicLong

  def nextId: Long = id.getAndIncrement + nodeId
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object RemoteClient extends Logging {
 
  private[amqp] val bridge = AMQPBridge.getAMQPBridge(config)

  private val remoteClients = new HashMap[String, RemoteClient]
  private val remoteActors = new HashMap[RemoteServer.Address, HashSet[String]]

  // FIXME: simplify overloaded methods when we have Scala 2.8

  def actorFor(className: String, hostname: String, port: Int): Actor =
    actorFor(className, className, 5000L, hostname, port)

  def actorFor(actorId: String, className: String, hostname: String, port: Int): Actor =
    actorFor(actorId, className, 5000L, hostname, port)

  def actorFor(className: String, timeout: Long, hostname: String, port: Int): Actor =
    actorFor(className, className, timeout, hostname, port)

  def actorFor(actorId: String, className: String, timeout: Long, hostname: String, port: Int): Actor = {
    new Actor {
      start
      val remoteClient = RemoteClient.clientFor(hostname, port)

      override def postMessageToMailbox(message: Any, sender: Option[Actor]): Unit = {
        val requestBuilder = RemoteRequest.newBuilder
            .setId(RemoteRequestIdFactory.nextId)
            .setTarget(className)
            .setTimeout(timeout)
            .setUuid(actorId)
            .setIsActor(true)
            .setIsOneWay(true)
            .setIsEscaped(false)
        if (sender.isDefined) {
          val s = sender.get
          requestBuilder.setSourceTarget(s.getClass.getName)
          requestBuilder.setSourceUuid(s.uuid)
          val (host, port) = s._replyToAddress.map(a => (a.getHostName, a.getPort)).getOrElse((Actor.HOSTNAME, Actor.PORT))
          requestBuilder.setSourceHostname(host)
          requestBuilder.setSourcePort(port)
        }
        RemoteProtocolBuilder.setMessage(message, requestBuilder)
        remoteClient.send(requestBuilder.build, None)
      }

      override def postMessageToMailboxAndCreateFutureResultWithTimeout(
          message: Any,
          timeout: Long,
          senderFuture: Option[CompletableFuture]): CompletableFuture = {
        val requestBuilder = RemoteRequest.newBuilder
            .setId(RemoteRequestIdFactory.nextId)
            .setTarget(className)
            .setTimeout(timeout)
            .setUuid(actorId)
            .setIsActor(true)
            .setIsOneWay(false)
            .setIsEscaped(false)
        RemoteProtocolBuilder.setMessage(message, requestBuilder)
        val future = remoteClient.send(requestBuilder.build, senderFuture)
        if (future.isDefined) future.get
        else throw new IllegalStateException("Expected a future from remote call to actor " + toString)
      }

      def receive = {case _ => {}}
    }
  }

  def clientFor(hostname: String, port: Int): RemoteClient = clientFor(new InetSocketAddress(hostname, port))

  def clientFor(address: InetSocketAddress): RemoteClient = synchronized {
    val hostname = address.getHostName
    val port = address.getPort
    val hash = hostname + ':' + port
    if (remoteClients.contains(hash)) remoteClients(hash)
    else {
      val client = new RemoteClient(hostname, port)
      client.connect
      remoteClients += hash -> client
      client
    }
  }

  def shutdownClientFor(address: InetSocketAddress) = synchronized {
    val hostname = address.getHostName
    val port = address.getPort
    val hash = hostname + ':' + port
    if (remoteClients.contains(hash)) {
      val client = remoteClients(hash)
      client.shutdown
      remoteClients - hash
    }
  }

  /**
   * Clean-up all open connections.
   */
  def shutdownAll = synchronized {
    remoteClients.foreach({case (addr, client) => client.shutdown})
    remoteClients.clear
  }

  private[akka] def register(hostname: String, port: Int, uuid: String) = synchronized {
    actorsFor(RemoteServer.Address(hostname, port)) + uuid
  }

  // TODO: add RemoteClient.unregister for ActiveObject, but first need a @shutdown callback 
  private[akka] def unregister(hostname: String, port: Int, uuid: String) = synchronized {
    val set = actorsFor(RemoteServer.Address(hostname, port))
    set - uuid
    if (set.isEmpty) shutdownClientFor(new InetSocketAddress(hostname, port))
  }

  private[akka] def actorsFor(remoteServerAddress: RemoteServer.Address): HashSet[String] = {
    val set = remoteActors.get(remoteServerAddress)
    if (set.isDefined && (set.get ne null)) set.get
    else {
      val remoteActorSet = new HashSet[String]
      remoteActors.put(remoteServerAddress, remoteActorSet)
      remoteActorSet
    }
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class RemoteClient(hostname: String, port: Int) extends Logging {
  private var name = ""

  @volatile private[remote] var isRunning = false
  private val futures = new ConcurrentHashMap[Long, CompletableFuture]
  private val supervisors = new ConcurrentHashMap[String, Actor]

  private val timer = new HashedWheelTimer
  private val remoteAddress = new InetSocketAddress(hostname, port)

  private var channel: AMQPChannel = null
  private var reqQueueName: String = null
  private var repQueueName: String = null
  private var exchangeName: String = null

  def connect = synchronized {
    if (!isRunning) {
      log.info("Starting remote client connection to [%s:%s]", hostname, port)
      name = hostname + ":" + port
      createAMQPChannel      
      isRunning = true
    }
  }

  private [amqp] def createAMQPChannel(): Unit = {
    repQueueName = "actorQueueFor_"+name + "Rep"
    reqQueueName = "actorQueueFor_"+name + "Req"
    exchangeName = "actorExchangeFor_"+name
    log.info("client Setting up Channel to Queue,Exchange to: [%s,%s,%s] ",repQueueName, reqQueueName , exchangeName)
    channel = RemoteClient.bridge.connection.createChannel()
    channel.exchangeDeclare(exchangeName, "direct")
    channel.queueDeclare(repQueueName)
    channel.queueDeclare(reqQueueName)
    channel.queueBind(repQueueName, exchangeName, "remoteReply")
    channel.queueBind(reqQueueName, exchangeName, "remoteRequest")
    channel.basicConsume(repQueueName, false, new ClientAMQPQueueConsumer(name, futures, supervisors, channel, this))
    log.info("Done!")
  }

  def shutdown = synchronized {
    if (isRunning) {
      isRunning = false
      //openChannels.close.awaitUninterruptibly
      //bootstrap.releaseExternalResources
      timer.stop
      log.info("%s has been shut down", name)
    }
  }

  def send(request: RemoteRequest, senderFuture: Option[CompletableFuture]): Option[CompletableFuture] = if (isRunning) {
    if (request.getIsOneWay) {
      channel.basicPublish(exchangeName,"remoteRequest",null,request.toByteArray)
      None
    } else {
      futures.synchronized {
        val futureResult = if (senderFuture.isDefined) senderFuture.get
        else new DefaultCompletableFuture(request.getTimeout)
        futures.put(request.getId, futureResult)
        channel.basicPublish(exchangeName,"remoteRequest",null,request.toByteArray)
        Some(futureResult)
      }
    }
  } else throw new IllegalStateException("Remote client is not running, make sure you have invoked 'RemoteClient.connect' before using it.")

  def registerSupervisorForActor(actor: Actor) =
    if (!actor._supervisor.isDefined) throw new IllegalStateException("Can't register supervisor for " + actor + " since it is not under supervision")
    else supervisors.putIfAbsent(actor._supervisor.get.uuid, actor)

  def deregisterSupervisorForActor(actor: Actor) =
    if (!actor._supervisor.isDefined) throw new IllegalStateException("Can't unregister supervisor for " + actor + " since it is not under supervision")
    else supervisors.remove(actor._supervisor.get.uuid)
  def deregisterSupervisorWithUuid(uuid: String) = supervisors.remove(uuid)

}

class ClientAMQPQueueConsumer(val name: String,
                          val futures: ConcurrentMap[Long, CompletableFuture],
                          val supervisors: ConcurrentMap[String, Actor],
                          val channel: AMQPChannel,
                          val client: RemoteClient)
    extends DefaultConsumer(channel) with Logging {

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) = {
    super.handleShutdownSignal(consumerTag, sig)
  }

  override def handleCancelOk(consumerTag: String) = {
    super.handleCancelOk(consumerTag)
  }

  override def handleConsumeOk(consumerTag: String) = {
    super.handleConsumeOk(consumerTag)
  }

   override def handleDelivery(consumerTag: String,
                              envelope: Envelope,
                              properties: BasicProperties,
                              body: Array[Byte]) = {
    import Actor.Sender.Self 
    try {
      //val result = body
      //if (result.isInstanceOf[RemoteReply]) {
        val reply = RemoteReply.parseFrom(body)//result.asInstanceOf[RemoteReply]
        // TODO error handling
        log.debug("Remote client received RemoteReply[\n%s]", reply.toString)
        val future = futures.get(reply.getId)
        if (reply.getIsSuccessful) {
          val message = RemoteProtocolBuilder.getMessage(reply)
          future.completeWithResult(message)
        } else {
          if (reply.hasSupervisorUuid()) {
            val supervisorUuid = reply.getSupervisorUuid
            if (!supervisors.containsKey(supervisorUuid)) throw new IllegalStateException("Expected a registered supervisor for UUID [" + supervisorUuid + "] but none was found")
            val supervisedActor = supervisors.get(supervisorUuid)
            if (!supervisedActor._supervisor.isDefined) throw new IllegalStateException("Can't handle restart for remote actor " + supervisedActor + " since its supervisor has been removed")
            else {
             val _actor:Actor = supervisedActor._supervisor.get
             _actor ! Exit(supervisedActor, parseException(reply))
            }
          }
          future.completeWithException(null, parseException(reply))
        }
        futures.remove(reply.getId)
      //} else throw new IllegalArgumentException("Unknown message received in remote client handler: " + result)
    } catch {
      case e: Exception =>
        log.error("Unexpected exception in remote client handler: %s", e)
        throw e
    }
  }

  private def parseException(reply: RemoteReply) = {
    val exception = reply.getException
    val exceptionType = Class.forName(exception.substring(0, exception.indexOf('$')))
    val exceptionMessage = exception.substring(exception.indexOf('$') + 1, exception.length)
    exceptionType
        .getConstructor(Array[Class[_]](classOf[String]): _*)
        .newInstance(exceptionMessage).asInstanceOf[Throwable]
  }

/*  override def channelClosed(ctx: ChannelHandlerContext, event: ChannelStateEvent) = if (client.isRunning) {
    timer.newTimeout(new TimerTask() {
      def run(timeout: Timeout) = {
        log.debug("Remote client reconnecting to [%s]", remoteAddress)
        //client.connection = bootstrap.connect(remoteAddress)

        // Wait until the connection attempt succeeds or fails.
        //client.connection.awaitUninterruptibly
        //if (!client.connection.isSuccess) log.error(client.connection.getCause, "Reconnection to [%s] has failed", remoteAddress)
      }
    }, RemoteClient.RECONNECT_DELAY, TimeUnit.MILLISECONDS)
  }*/
}
