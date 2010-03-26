package se.scalablesolutions.akka.remote.amqp

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.{ConcurrentHashMap}
import java.util.{Map => JMap}

import se.scalablesolutions.akka.actor._
import se.scalablesolutions.akka.util._
import se.scalablesolutions.akka.remote.protobuf.RemoteProtocol.{RemoteReply, RemoteRequest}
import se.scalablesolutions.akka.config.Config.config
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.ChannelGroup


/**
 * Use this object if you need a single remote server on a specific node.
 *
 * <pre>
 * // takes hostname and port from 'akka.conf'
 * RemoteNode.start
 * </pre>
 *
 * <pre>
 * RemoteNode.start(hostname, port)
 * </pre>
 * 
 * You can specify the class loader to use to load the remote actors.
 * <pre>
 * RemoteNode.start(hostname, port, classLoader)
 * </pre>
 *
 * If you need to create more than one, then you can use the RemoteServer:
 *
 * <pre>
 * val server = new RemoteServer
 * server.start(hostname, port)
 * </pre>
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object RemoteNode extends RemoteServer

/**
 * For internal use only.
 * Holds configuration variables, remote actors, remote active objects and remote servers.
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object RemoteServer {
  val HOSTNAME = config.getString("akka.remote.server.hostname", "localhost")
  val PORT = config.getInt("akka.remote.server.port", 9999)

  private val bootstrap = {
    val _bootstrap = new AMQPBootstrap(config)
    _bootstrap boot;
    _bootstrap start;
    _bootstrap
  }

  object Address {
    def apply(hostname: String, port: Int) = new Address(hostname, port)
  }
  
  class Address(val hostname: String, val port: Int) {
    override def hashCode: Int = {
      var result = HashCode.SEED
      result = HashCode.hash(result, hostname)
      result = HashCode.hash(result, port)
      result
    }
    override def equals(that: Any): Boolean = {
      that != null &&
      that.isInstanceOf[Address] &&
      that.asInstanceOf[Address].hostname == hostname &&
      that.asInstanceOf[Address].port == port
    }
  }
  
  class RemoteActorSet {
    val actors =        new ConcurrentHashMap[String, Actor]
    val activeObjects = new ConcurrentHashMap[String, AnyRef]    
  }

  private val remoteActorSets = new ConcurrentHashMap[Address, RemoteActorSet]
  private val remoteServers = new ConcurrentHashMap[Address, RemoteServer]
    
  private[akka] def actorsFor(remoteServerAddress: RemoteServer.Address): RemoteActorSet = {
    val set = remoteActorSets.get(remoteServerAddress)
    if (set ne null) set
    else {
      val remoteActorSet = new RemoteActorSet
      remoteActorSets.put(remoteServerAddress, remoteActorSet)
      remoteActorSet
    }
  }

  private[remote] def serverFor(hostname: String, port: Int): Option[RemoteServer] = {
    val server = remoteServers.get(Address(hostname, port))
    if (server eq null) None
    else Some(server)
  }

  private[remote] def register(hostname: String, port: Int, server: RemoteServer) =
    remoteServers.put(Address(hostname, port), server)
    
  private[remote] def unregister(hostname: String, port: Int) =
    remoteServers.remove(Address(hostname, port))
}

/**
 * Use this class if you need a more than one remote server on a specific node.
 *
 * <pre>
 * val server = new RemoteServer
 * server.start
 * </pre>
 *
 * If you need to create more than one, then you can use the RemoteServer:
 *
 * <pre>
 * RemoteNode.start
 * </pre>
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class RemoteServer extends Logging {
  val name = "RemoteServer@" + hostname + ":" + port

  private var hostname = RemoteServer.HOSTNAME
  private var port =     RemoteServer.PORT

  @volatile private var isRunning = false
  @volatile private var isConfigured = false

  def start: Unit = start(None)
               
  def start(loader: Option[ClassLoader]): Unit = start(hostname, port, loader)

  def start(_hostname: String, _port: Int): Unit = start(_hostname, _port, None)

  def start(_hostname: String, _port: Int, loader: Option[ClassLoader]): Unit = synchronized {
    try {
      if (!isRunning) {
        hostname = _hostname
        port = _port
        log.info("Starting remote server at [%s:%s]", hostname, port)
        RemoteServer.register(hostname, port, this)
        val remoteActorSet = RemoteServer.actorsFor(RemoteServer.Address(hostname, port))
        isRunning = true
        Cluster.registerLocalNode(hostname, port)
      }      
    } catch {
      case e => log.error(e, "Could not start up remote server")
    }
  }

  def shutdown = if (isRunning) {
    RemoteServer.unregister(hostname, port)
    // TODO identificar qdo derrubar o cara -- bootstrap.releaseExternalResources
    Cluster.deregisterLocalNode(hostname, port)
  }

  // TODO: register active object in RemoteServer as well

  /**
   * Register Remote Actor by the Actor's 'id' field.
   */
  def register(actor: Actor) = if (isRunning) {
    log.info("Registering server side remote actor [%s] with id [%s]", actor.getClass.getName, actor.getId)
    RemoteServer.actorsFor(RemoteServer.Address(hostname, port)).actors.put(actor.getId, actor)
  }

  /**
   * Register Remote Actor by a specific 'id' passed as argument. 
   */
  def register(id: String, actor: Actor) = if (isRunning) {
    log.info("Registering server side remote actor [%s] with id [%s]", actor.getClass.getName, id)
    RemoteServer.actorsFor(RemoteServer.Address(hostname, port)).actors.put(id, actor)
  }
}

class RemoteServerHandler(
    val name: String,
    val openChannels: ChannelGroup,
    val applicationLoader: Option[ClassLoader],
    val actors: JMap[String, Actor],
    val activeObjects: JMap[String, AnyRef]) extends SimpleChannelUpstreamHandler with Logging {
  val AW_PROXY_PREFIX = "$$ProxiedByAW".intern

  applicationLoader.foreach(RemoteProtocolBuilder.setClassLoader(_))

  override def channelOpen(ctx: ChannelHandlerContext, event: ChannelStateEvent) {
    openChannels.add(ctx.getChannel)
  }

  override def handleUpstream(ctx: ChannelHandlerContext, event: ChannelEvent) = {
    if (event.isInstanceOf[ChannelStateEvent] &&
        event.asInstanceOf[ChannelStateEvent].getState != ChannelState.INTEREST_OPS) {
      log.debug(event.toString)
    }
    super.handleUpstream(ctx, event)
  }

  override def messageReceived(ctx: ChannelHandlerContext, event: MessageEvent) = {
    val message = event.getMessage
    if (message eq null) throw new IllegalStateException("Message in remote MessageEvent is null: " + event)
    if (message.isInstanceOf[RemoteRequest]) {
      handleRemoteRequest(message.asInstanceOf[RemoteRequest], event.getChannel)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent) = {
    log.error(event.getCause, "Unexpected exception from remote downstream")
    event.getChannel.close
  }

  private def handleRemoteRequest(request: RemoteRequest, channel: Channel) = {
    log.debug("Received RemoteRequest[\n%s]", request.toString)
    if (request.getIsActor) dispatchToActor(request, channel)
    else dispatchToActiveObject(request, channel)
  }

  private def dispatchToActor(request: RemoteRequest, channel: Channel) = {
    log.debug("Dispatching to remote actor [%s]", request.getTarget)
    val actor = createActor(request.getTarget, request.getUuid, request.getTimeout)
    
    val message = RemoteProtocolBuilder.getMessage(request)
    if (request.getIsOneWay) {
      if (request.hasSourceHostname && request.hasSourcePort) {
        // re-create the sending actor
        val targetClass = if (request.hasSourceTarget) request.getSourceTarget
        else request.getTarget

        val remoteActor = createActor(targetClass, request.getSourceUuid, request.getTimeout)
        if (!remoteActor.isRunning) {
          remoteActor.makeRemote(request.getSourceHostname, request.getSourcePort)
          remoteActor.start
        }
        actor.!(message)(Some(remoteActor))
      } else {
        // couldn't find a way to reply, send the message without a source/sender
        actor.send(message)
      }
    } else {
      try {
        val resultOrNone = actor !! message
        val result: AnyRef = if (resultOrNone.isDefined) resultOrNone.get else null
        log.debug("Returning result from actor invocation [%s]", result)
        val replyBuilder = RemoteReply.newBuilder
            .setId(request.getId)
            .setIsSuccessful(true)
            .setIsActor(true)
        RemoteProtocolBuilder.setMessage(result, replyBuilder)
        if (request.hasSupervisorUuid) replyBuilder.setSupervisorUuid(request.getSupervisorUuid)
        val replyMessage = replyBuilder.build
        channel.write(replyMessage)
      } catch {
        case e: Throwable =>
          log.error(e, "Could not invoke remote actor [%s]", request.getTarget)
          val replyBuilder = RemoteReply.newBuilder
              .setId(request.getId)
              .setException(e.getClass.getName + "$" + e.getMessage)
              .setIsSuccessful(false)
              .setIsActor(true)
          if (request.hasSupervisorUuid) replyBuilder.setSupervisorUuid(request.getSupervisorUuid)
          val replyMessage = replyBuilder.build
          channel.write(replyMessage)
      }
    }
  }

  private def dispatchToActiveObject(request: RemoteRequest, channel: Channel) = {
    log.debug("Dispatching to remote active object [%s :: %s]", request.getMethod, request.getTarget)
    val activeObject = createActiveObject(request.getTarget, request.getTimeout)

    val args = RemoteProtocolBuilder.getMessage(request).asInstanceOf[Array[AnyRef]].toList
    val argClasses = args.map(_.getClass)
    val (unescapedArgs, unescapedArgClasses) = unescapeArgs(args, argClasses, request.getTimeout)

    //continueTransaction(request)
    try {
      val messageReceiver = activeObject.getClass.getDeclaredMethod(
        request.getMethod, unescapedArgClasses: _*)
      if (request.getIsOneWay) messageReceiver.invoke(activeObject, unescapedArgs: _*)
      else {
        val result = messageReceiver.invoke(activeObject, unescapedArgs: _*)
        log.debug("Returning result from remote active object invocation [%s]", result)
        val replyBuilder = RemoteReply.newBuilder
            .setId(request.getId)
            .setIsSuccessful(true)
            .setIsActor(false)
        RemoteProtocolBuilder.setMessage(result, replyBuilder)
        if (request.hasSupervisorUuid) replyBuilder.setSupervisorUuid(request.getSupervisorUuid)
        val replyMessage = replyBuilder.build
        channel.write(replyMessage)
      }
    } catch {
      case e: InvocationTargetException =>
        log.error(e.getCause, "Could not invoke remote active object [%s :: %s]", request.getMethod, request.getTarget)
        val replyBuilder = RemoteReply.newBuilder
            .setId(request.getId)
            .setException(e.getCause.getClass.getName + "$" + e.getCause.getMessage)
            .setIsSuccessful(false)
            .setIsActor(false)
        if (request.hasSupervisorUuid) replyBuilder.setSupervisorUuid(request.getSupervisorUuid)
        val replyMessage = replyBuilder.build
        channel.write(replyMessage)
      case e: Throwable =>
        log.error(e.getCause, "Could not invoke remote active object [%s :: %s]", request.getMethod, request.getTarget)
        val replyBuilder = RemoteReply.newBuilder
            .setId(request.getId)
            .setException(e.getClass.getName + "$" + e.getMessage)
            .setIsSuccessful(false)
            .setIsActor(false)
        if (request.hasSupervisorUuid) replyBuilder.setSupervisorUuid(request.getSupervisorUuid)
        val replyMessage = replyBuilder.build
        channel.write(replyMessage)
    }
  }

  /*
  private def continueTransaction(request: RemoteRequest) = {
    val tx = request.tx
    if (tx.isDefined) {
      tx.get.reinit
      TransactionManagement.threadBoundTx.set(tx)
      setThreadLocalTransaction(tx.transaction)
    } else {
      TransactionManagement.threadBoundTx.set(None)     
      setThreadLocalTransaction(null)
    }
  }
  */
  private def unescapeArgs(args: scala.List[AnyRef], argClasses: scala.List[Class[_]], timeout: Long) = {
    val unescapedArgs = new Array[AnyRef](args.size)
    val unescapedArgClasses = new Array[Class[_]](args.size)

    val escapedArgs = for (i <- 0 until args.size) {
      val arg = args(i)
      if (arg.isInstanceOf[String] && arg.asInstanceOf[String].startsWith(AW_PROXY_PREFIX)) {
        val argString = arg.asInstanceOf[String]
        val proxyName = argString.replace(AW_PROXY_PREFIX, "") //argString.substring(argString.indexOf("$$ProxiedByAW"), argString.length)
        val activeObject = createActiveObject(proxyName, timeout)
        unescapedArgs(i) = activeObject
        unescapedArgClasses(i) = Class.forName(proxyName)
      } else {
        unescapedArgs(i) = args(i)
        unescapedArgClasses(i) = argClasses(i)
      }
    }
    (unescapedArgs, unescapedArgClasses)
  }

  private def createActiveObject(name: String, timeout: Long): AnyRef = {
    val activeObjectOrNull = activeObjects.get(name)
    if (activeObjectOrNull eq null) {
      try {
        log.info("Creating a new remote active object [%s]", name)
        val clazz = if (applicationLoader.isDefined) applicationLoader.get.loadClass(name)
        else Class.forName(name)
        val newInstance = ActiveObject.newInstance(clazz, timeout).asInstanceOf[AnyRef]
        activeObjects.put(name, newInstance)
        newInstance
      } catch {
        case e =>
          log.error(e, "Could not create remote active object instance")
          throw e
      }
    } else activeObjectOrNull
  }

  private def createActor(name: String, uuid: String, timeout: Long): Actor = {
    val actorOrNull = actors.get(uuid)
    if (actorOrNull eq null) {
      try {
        log.info("Creating a new remote actor [%s:%s]", name, uuid)
        val clazz = if (applicationLoader.isDefined) applicationLoader.get.loadClass(name)
        else Class.forName(name)
        val newInstance = clazz.newInstance.asInstanceOf[Actor]
        newInstance._uuid = uuid
        newInstance.timeout = timeout
        newInstance._remoteAddress = None
        actors.put(uuid, newInstance)
        newInstance.start
        newInstance
      } catch {
        case e =>
          log.error(e, "Could not create remote actor instance")
          throw e
      }
    } else actorOrNull
  }
}
