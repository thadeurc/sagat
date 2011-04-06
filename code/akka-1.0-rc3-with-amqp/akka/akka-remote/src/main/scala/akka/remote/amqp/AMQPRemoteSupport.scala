package akka.remote.amqp

import akka.remoteinterface._
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import akka.remote.MessageSerializer
import collection.mutable.HashMap
import akka.util._
import akka.actor._
import akka.actor.{ActorType => AkkaActorType}
import akka.serialization.RemoteActorSerialization._
import akka.serialization.RemoteActorSerialization
import java.lang.reflect.InvocationTargetException
import akka.dispatch.{Future, DefaultCompletableFuture, CompletableFuture}
import akka.remote.RemoteServerSettings._
import akka.remote.protocol.RemoteProtocol._
import akka.remote.protocol.RemoteProtocol.ActorType._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.{Either}

object AMQPUtil {
  import AMQPBridge._
  import ConnectionSharePolicy._
  import StorageAndConsumptionPolicy._

  def makeName(host: String, port: Int) = "%s@%d".format(host, port)
  def makeName(remoteAddress: InetSocketAddress): String = makeName(remoteAddress.getHostName, remoteAddress.getPort)
  implicit def asInetAddress(name: String): InetSocketAddress = {
    val list = name.split("@")
    if(list.length == 2){
      InetSocketAddress.createUnresolved(list(0), list(1).toInt)
    }
    else throw new IllegalArgumentException("String %s cannot be converted as InetAddress".format(name))
  }

  private[akka] lazy val storagePolicy = {
    var param = ReflectiveAccess.Remote.STORAGE_AND_CONSUME_POLICY
    var result = EXCLUSIVE_TRANSIENT
    try{
      result = StorageAndConsumptionPolicy.withName(param).asInstanceOf[MessageStorageAndConsumptionPolicyParams]
    }catch {
      case e: NoSuchElementException =>
        log.warn("Value [{}] for param 'akka.remote.layer.amqp.storage.policy' is not valid. " +
          "It should be either {}.\nUsing default value.",
          Array[Any](param, StorageAndConsumptionPolicy.values.toList))
    }
    result
  }

  private[akka] lazy val serverConnectionPolicy = {
    var param  = ReflectiveAccess.Remote.SERVER_CONNECTION_POLICY
    var result = ONE_CONN_PER_NODE
    try{
      result = ConnectionSharePolicy.withName(param).asInstanceOf[ConnectionSharePolicyParams]
    }catch {
      case e: NoSuchElementException =>
        log.warn("Value [{}] for param 'akka.remote.layer.amqp.server.connection.policy' is not valid. " +
          "It should be either {}.\nUsing default value.",
          Array[Any](param, ConnectionSharePolicy.values.toList))
    }
    result
  }

  private[akka] lazy val clientConnectionPolicy = {
    var param  = ReflectiveAccess.Remote.CLIENT_CONNECTION_POLICY
    var result = ONE_CONN_PER_NODE
    try{
      result = ConnectionSharePolicy.withName(param).asInstanceOf[ConnectionSharePolicyParams]
    }catch {
      case e: NoSuchElementException =>
        log.warn("Value [{}] for param 'akka.remote.layer.amqp.client.connection.policy' is not valid. " +
          "It should be either {}.\nUsing default value.",
          Array[Any](param, ConnectionSharePolicy.values.toList))
    }
    result
  }
}

trait AMQPRemoteClientModule extends RemoteClientModule { self: ListenerManagement with Logging =>
  import AMQPUtil._
  private val remoteClients = new HashMap[String, AMQPRemoteClient]
  private val remoteActors  = new Index[String, Uuid]
  private val lock          = new ReadWriteGuard

  private[akka] def registerClientManagedActor(nodeName: String, uuid: Uuid) {
    remoteActors.put(nodeName, uuid)
  }

  private[akka] def registerClientManagedActor(hostName: String, port: Int, uuid: Uuid) {
    registerClientManagedActor(makeName(hostName, port), uuid)
  }

  private[akka] def unregisterClientManagedActor(nodeName: String, uuid: Uuid) {
    remoteActors.remove(nodeName, uuid)
  }

  private[akka] def unregisterClientManagedActor(hostName: String, port: Int, uuid: Uuid) {
    unregisterClientManagedActor(makeName(hostName, port), uuid)
  }

  private[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef =
    withClientFor(makeName(actorRef.homeAddress.get), None)(_.registerSupervisorForActor(actorRef))

  private[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef = lock withReadGuard {
    remoteClients.get(makeName(actorRef.homeAddress.get)) match {
      case Some(client) => client.deregisterSupervisorForActor(actorRef)
      case None => actorRef
    }
  }

  protected[akka] def send[T](message: Any,
                              senderOption: Option[ActorRef],
                              senderFuture: Option[CompletableFuture[T]],
                              nodeName: String,
                              timeout: Long,
                              isOneWay: Boolean,
                              actorRef: ActorRef,
                              typedActorInfo: Option[(String, String)],
                              actorType: AkkaActorType,
                              loader: Option[ClassLoader]): Option[CompletableFuture[T]] = {
    withClientFor(nodeName, loader)(_.send[T](message, senderOption, senderFuture, nodeName, timeout, isOneWay, actorRef, typedActorInfo, actorType))
  }

  protected[akka] def send[T](message: Any,
                              senderOption: Option[ActorRef],
                              senderFuture: Option[CompletableFuture[T]],
                              remoteAddress: InetSocketAddress,
                              timeout: Long,
                              isOneWay: Boolean,
                              actorRef: ActorRef,
                              typedActorInfo: Option[(String, String)],
                              actorType: AkkaActorType,
                              loader: Option[ClassLoader]): Option[CompletableFuture[T]] = {
    send[T](message, senderOption, senderFuture, makeName(remoteAddress), timeout, isOneWay, actorRef, typedActorInfo, actorType, loader)
  }

  private[akka] def withClientFor[T](
    nodeName: String, loader: Option[ClassLoader])(function: AMQPRemoteClient => T): T = {
    loader.foreach(MessageSerializer.setClassLoader(_))
    lock.readLock.lock
    try {
      val c = remoteClients.get(nodeName) match {
        case Some(client) => client
        case None =>
          lock.readLock.unlock
          lock.writeLock.lock //Lock upgrade, not supported natively
          try {
            try {
              remoteClients.get(nodeName) match { //Recheck for addition, race between upgrades
                case Some(client) => client //If already populated by other writer
                case None => //Populate map
                  val client = new AMQPRemoteClient(this, nodeName, loader, self.notifyListeners _)
                  client.connect(false)
                  remoteClients += nodeName -> client
                  client
              }
            } finally { lock.readLock.lock } //downgrade
          } finally { lock.writeLock.unlock }
      }
      function(c)
    }
    finally { lock.readLock.unlock }
  }

  protected[akka] def typedActorFor[T](intfClass: Class[T], serviceId: String, implClassName: String, timeout: Long, host: String, port: Int, loader: Option[ClassLoader]) =
    TypedActor.createProxyForRemoteActorRef(intfClass, RemoteActorRef(serviceId, implClassName, host, port, timeout, loader, AkkaActorType.TypedActor))


  def shutdownClientConnection(nodeName: String): Boolean = lock withWriteGuard {
    remoteClients.remove(nodeName) match {
      case Some(client) => client.shutdown
      case None => false
    }
  }

  def shutdownClientConnection(address: InetSocketAddress): Boolean =  {
    shutdownClientConnection(makeName(address.getHostName, address.getPort))
  }

  def restartClientConnection(nodeName: String): Boolean =  lock withReadGuard {
    remoteClients.get(nodeName) match {
      // TODO implement reconnect
      case Some(client) => true//client.connect(reconnectIfAlreadyConnected = true)
      case None => false
    }
  }

  def restartClientConnection(address: InetSocketAddress): Boolean = {
    restartClientConnection(makeName(address.getHostName, address.getPort))
  }

  def shutdownClientModule = {
    shutdownRemoteClients
  }

  def shutdownRemoteClients = lock withWriteGuard {
    remoteClients.foreach({ case (_, client) => client.shutdown })
    remoteClients.clear
  }
}

class AMQPRemoteClient(clientModule: AMQPRemoteClientModule, val nodeName: String, val loader: Option[ClassLoader],
                       notifyListenersFunction: (=> Any) => Unit) extends Logging with MessageHandler {
  import AMQPBridge._
  import AMQPUtil._

  loader.foreach(MessageSerializer.setClassLoader(_))

  @volatile private var amqpClientBridge: ClientAMQPBridge  = _
  protected val futures = new ConcurrentHashMap[Uuid, CompletableFuture[_]]
  protected val supervisors = new ConcurrentHashMap[Uuid, ActorRef]
  private[remote] val runSwitch = new Switch
  private[remote] def isRunning = runSwitch.isOn
  protected def notifyListeners(msg: => Any): Unit = notifyListenersFunction

  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean = {
    runSwitch switchOn {
      // TODO fazer o tratamento de erro
      amqpClientBridge = createAMQPRemoteClientBridge(nodeName)
      notifyListeners(RemoteClientStarted(clientModule, nodeName))
    }
    true
  }

  def shutdown: Boolean =
    runSwitch switchOff {
    log.slf4j.info("Shutting down {}", nodeName)
    notifyListeners(RemoteClientShutdown(clientModule, nodeName))
    amqpClientBridge.shutdown
    amqpClientBridge = null
    log.slf4j.info("{} has been shut down", nodeName)
  }

  private def createAMQPRemoteClientBridge(nodeName: String): ClientAMQPBridge = {
    //val handler = new ClientMessageHandler(clientModule, loader, futures, supervisors)
    //val bridge = newClientBridge(nodeName, this, storagePolicy, clientConnectionPolicy)
    /* FIXME improve - the api for bridge is very fragile */
    //handler.bridge = bridge
    //bridge
    newClientBridge(nodeName, this, storagePolicy, clientConnectionPolicy)
  }

  def send[T](
    message: Any,
    senderOption: Option[ActorRef],
    senderFuture: Option[CompletableFuture[T]],
    nodeName: String,
    timeout: Long,
    isOneWay: Boolean,
    actorRef: ActorRef,
    typedActorInfo: Option[Tuple2[String, String]],
    actorType: AkkaActorType): Option[CompletableFuture[T]] = synchronized {

    send(createRemoteMessageProtocolBuilder(
        Some(actorRef),
        Left(actorRef.uuid),
        actorRef.id,
        actorRef.actorClassName,
        actorRef.timeout,
        Left(message),
        isOneWay,
        senderOption,
        typedActorInfo,
        actorType,
        None,
        Some(amqpClientBridge.id)
      ).build, senderFuture)
  }

  def send[T](
    request: RemoteMessageProtocol,
    senderFuture: Option[CompletableFuture[T]]): Option[CompletableFuture[T]] = {
    log.slf4j.debug("sending message: {} has future {}", request, senderFuture)
    if (isRunning) {
      if (request.getOneWay) {
        amqpClientBridge.sendMessageToServer(request.toByteArray)
        None
      } else {
          val futureResult = if (senderFuture.isDefined) senderFuture.get
                             else new DefaultCompletableFuture[T](request.getActorInfo.getTimeout)
          amqpClientBridge.sendMessageToServer(request.toByteArray)
          val futureUuid = uuidFrom(request.getUuid.getHigh, request.getUuid.getLow)
          futures.put(futureUuid, futureResult)
          Some(futureResult)
      }
    } else {
      val exception = new RemoteClientException("Remote client is not running, make sure you have invoked " +
        "'RemoteClient.connect' before using it.", clientModule, nodeName)
      notifyListeners(RemoteClientError(exception, clientModule, nodeName))
      throw exception
    }
  }

  private[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef =
    if (!actorRef.supervisor.isDefined) throw new IllegalActorStateException(
      "Can't register supervisor for " + actorRef + " since it is not under supervision")
    else supervisors.putIfAbsent(actorRef.supervisor.get.uuid, actorRef)

  private[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef =
    if (!actorRef.supervisor.isDefined) throw new IllegalActorStateException(
      "Can't unregister supervisor for " + actorRef + " since it is not under supervision")
    else supervisors.remove(actorRef.supervisor.get.uuid)

  def handleMessageReceived(message: Array[Byte]): Boolean = {
    if(message == null) throw new IllegalActorStateException("Received null message in AMQP Remote Client is null")
    try {
      val reply = RemoteMessageProtocol.parseFrom(message)
      val replyUuid = uuidFrom(reply.getActorInfo.getUuid.getHigh, reply.getActorInfo.getUuid.getLow)
        log.slf4j.debug("Remote client received RemoteMessageProtocol[\n{}]",reply)
        log.slf4j.debug("Trying to map back to future: {}",replyUuid)
        val future = futures.remove(replyUuid).asInstanceOf[CompletableFuture[Any]]
        if (reply.hasMessage) {
          if (future eq null) throw new IllegalActorStateException("Future mapped to UUID " + replyUuid + " does not exist")
          val message = MessageSerializer.deserialize(reply.getMessage)
          future.completeWithResult(message)
        } else {
          val exception = parseException(reply, loader)
          if (reply.hasSupervisorUuid()) {
            val supervisorUuid = uuidFrom(reply.getSupervisorUuid.getHigh, reply.getSupervisorUuid.getLow)
            if (!supervisors.containsKey(supervisorUuid)) throw new IllegalActorStateException(
              "Expected a registered supervisor for UUID [" + supervisorUuid + "] but none was found")
            val supervisedActor = supervisors.get(supervisorUuid)
            if (!supervisedActor.supervisor.isDefined) throw new IllegalActorStateException(
              "Can't handle restart for remote actor " + supervisedActor + " since its supervisor has been removed")
            else supervisedActor.supervisor.get ! Exit(supervisedActor, exception)
          }
          future.completeWithException(exception)
        }
        true
    } catch {
      case e: Exception =>
        clientModule.notifyListeners(RemoteClientError(e, clientModule, nodeName))
        log.slf4j.error("Unexpected exception in remote client handler", e)
        false
        // throw e ???
    }
  }

  private def parseException(reply: RemoteMessageProtocol, loader: Option[ClassLoader]): Throwable = {
    val exception = reply.getException
    val classname = exception.getClassname
    try {
      val exceptionClass = if (loader.isDefined) loader.get.loadClass(classname)
                           else Class.forName(classname)
      exceptionClass
        .getConstructor(Array[Class[_]](classOf[String]): _*)
        .newInstance(exception.getMessage).asInstanceOf[Throwable]
    } catch {
      case problem =>
        log.debug("Couldn't parse exception returned from RemoteServer",problem)
        log.warn("Couldn't create instance of {} with message {}, returning UnparsableException",classname, exception.getMessage)
        UnparsableException(classname, exception.getMessage)
    }
  }

  def handleRejectedMessage(message: Array[Byte], clientId: String): Unit = {
    // TODO implement me!
  }


}

class AMQPRemoteSupport extends RemoteSupport with AMQPRemoteClientModule with AMQPRemoteServerModule{
  import AMQPUtil._
  val optimizeLocal = new AtomicBoolean(true)

  def optimizeLocalScoped_?() = optimizeLocal.get

  protected[akka] def actorFor(serviceId: String, className: String, timeout: Long, host: String, port: Int, loader: Option[ClassLoader]): ActorRef = {
    if (optimizeLocalScoped_?) {
      val home = makeName(this.address)
      if (home.equalsIgnoreCase(makeName(host, port))) {
        val localRef = findActorByIdOrUuid(serviceId,serviceId)
        if (localRef ne null) return localRef
      }
    }
    RemoteActorRef(serviceId, className, host, port, timeout, loader)
  }

  def clientManagedActorOf(factory: () => Actor, host: String, port: Int): ActorRef = {
    if (optimizeLocalScoped_?) {
      val home = makeName(this.address)
      if (home.equalsIgnoreCase(makeName(host, port)))
        return new LocalActorRef(factory, None)
    }
    new LocalActorRef(factory, Some(new InetSocketAddress(host, port)))
  }
}

trait AMQPRemoteServerModule extends RemoteServerModule { self: RemoteModule =>
  import AMQPUtil._
  private[akka] val currentServerBridge = new AtomicReference[Option[AMQPRemoteServer]](None)

  /* it does not make much sense for an amqp based transport - it would be better to return an string or an specific type */
  def address = currentServerBridge.get match {
    case Some(ref) => ref.nodeName
    case None => ReflectiveAccess.Remote.configDefaultAddress
  }

  def name = currentServerBridge.get match {
    case Some(s) => "AMQPRemoteServerModule:name=" + s.nodeName
    case None    => "AMQPRemoteServerModule:name=noname"
  }

  private val _isRunning = new Switch(false)

  def isRunning = _isRunning.isOn

  def start(_hostname: String, _port: Int, loader: Option[ClassLoader] = None): RemoteServerModule = {
    start(makeName(_hostname, _port), loader)
  }

  def start(nodeName: String, loader: Option[ClassLoader]): RemoteServerModule = guard withGuard {
    try {
      _isRunning switchOn {
        log.slf4j.debug("Starting up amqp remote server {}", nodeName)
        currentServerBridge.set(Some(new AMQPRemoteServer(this, nodeName, loader)))
      }
    } catch {
      case e =>
        log.slf4j.error("Could not start up amqp remote server", e)
        notifyListeners(RemoteServerError(e, this))
    }
    this
  }

  def shutdownServerModule = guard withGuard {
    _isRunning switchOff {
      currentServerBridge.getAndSet(None) foreach {
        bridge =>
        log.slf4j.debug("Shutting down amqp remote server {}", bridge.nodeName)
        bridge.shutdown
      }
    }
  }

  def registerTypedActor(id: String, typedActor: AnyRef): Unit = guard withGuard {
    log.slf4j.debug("Registering server side remote typed actor [{}] with id [{}]", typedActor.getClass.getName, id)
    if (id.startsWith(UUID_PREFIX)) registerTypedActor(id.substring(UUID_PREFIX.length), typedActor, typedActorsByUuid)
    else registerTypedActor(id, typedActor, typedActors)
  }

  def registerTypedPerSessionActor(id: String, factory: => AnyRef): Unit = guard withGuard {
    log.slf4j.debug("Registering server side typed remote session actor with id [{}]", id)
    registerTypedPerSessionActor(id, () => factory, typedActorsFactories)
  }

  def register(id: String, actorRef: ActorRef): Unit = guard withGuard {
    log.slf4j.debug("Registering server side remote actor [{}] with id [{}]", actorRef.actorClass.getName, id)
    if (id.startsWith(UUID_PREFIX)) register(id.substring(UUID_PREFIX.length), actorRef, actorsByUuid)
    else register(id, actorRef, actors)
  }

  def registerByUuid(actorRef: ActorRef): Unit = guard withGuard {
    log.slf4j.debug("Registering remote actor {} to it's uuid {}", actorRef, actorRef.uuid)
    register(actorRef.uuid.toString, actorRef, actorsByUuid)
  }

  private def register[Key](id: Key, actorRef: ActorRef, registry: ConcurrentHashMap[Key, ActorRef]) {
    if (_isRunning.isOn) {
      registry.putIfAbsent(id, actorRef)
      if (!actorRef.isRunning) actorRef.start
    }
  }

  def registerPerSession(id: String, factory: => ActorRef): Unit = synchronized {
    log.slf4j.debug("Registering server side remote session actor with id [{}]", id)
    registerPerSession(id, () => factory, actorsFactories)
  }

  private def registerPerSession[Key](id: Key, factory: () => ActorRef, registry: ConcurrentHashMap[Key,() => ActorRef]) {
    if (_isRunning.isOn)
      registry.putIfAbsent(id, factory)
  }

  private def registerTypedActor[Key](id: Key, typedActor: AnyRef, registry: ConcurrentHashMap[Key, AnyRef]) {
    if (_isRunning.isOn)
      registry.putIfAbsent(id, typedActor)
  }

  private def registerTypedPerSessionActor[Key](id: Key, factory: () => AnyRef, registry: ConcurrentHashMap[Key,() => AnyRef]) {
    if (_isRunning.isOn)
      registry.putIfAbsent(id, factory)
  }

  def unregister(actorRef: ActorRef): Unit = guard withGuard {
    if (_isRunning.isOn) {
      log.slf4j.debug("Unregistering server side remote actor [{}] with id [{}:{}]", Array[AnyRef](actorRef.actorClass.getName, actorRef.id, actorRef.uuid))
      actors.remove(actorRef.id, actorRef)
      actorsByUuid.remove(actorRef.uuid, actorRef)
    }
  }

  def unregister(id: String): Unit = guard withGuard {
    if (_isRunning.isOn) {
      log.slf4j.info("Unregistering server side remote actor with id [{}]", id)
      if (id.startsWith(UUID_PREFIX)) actorsByUuid.remove(id.substring(UUID_PREFIX.length))
      else {
        val actorRef = actors get id
        actorsByUuid.remove(actorRef.uuid, actorRef)
        actors.remove(id,actorRef)
      }
    }
  }

  def unregisterPerSession(id: String): Unit = {
    if (_isRunning.isOn) {
      log.slf4j.info("Unregistering server side remote session actor with id [{}]", id)
      actorsFactories.remove(id)
    }
  }

  def unregisterTypedActor(id: String):Unit = guard withGuard {
    if (_isRunning.isOn) {
      log.slf4j.info("Unregistering server side remote typed actor with id [{}]", id)
      if (id.startsWith(UUID_PREFIX)) typedActorsByUuid.remove(id.substring(UUID_PREFIX.length))
      else typedActors.remove(id)
    }
  }

  def unregisterTypedPerSessionActor(id: String): Unit =
    if (_isRunning.isOn) typedActorsFactories.remove(id)
}

class AMQPRemoteServer(val serverModule: AMQPRemoteServerModule, val nodeName: String, val loader: Option[ClassLoader])
  extends MessageHandler with Logging {
  import AMQPBridge._
  import AMQPUtil._

  loader.foreach(MessageSerializer.setClassLoader(_))

  private val amqpServerBridge = createAMQPRemoteServer(nodeName)
  private val sessionActors = new ConcurrentHashMap[String, ConcurrentHashMap[String, ActorRef]]()
  private val typedSessionActors = new ConcurrentHashMap[String, ConcurrentHashMap[String, AnyRef]]()


  def shutdown {
    try {
      amqpServerBridge.shutdown
      serverModule.notifyListeners(RemoteServerShutdown(serverModule))
    } catch {
      case e => serverModule.log.slf4j.warn("Could not close remote server channel in a graceful way")
    }
  }

  private def createAMQPRemoteServer(nodeName: String): ServerAMQPBridge = {
    //val handler = new ServerMessageHandler(serverModule, loader)
    val bridge = newServerBridge(nodeName, this, storagePolicy, serverConnectionPolicy)
    /* FIXME this api is very fragile */
    //handler.bridge = bridge
    serverModule.notifyListeners(RemoteServerStarted(serverModule))
    bridge
  }

  def handleMessageReceived(message: Array[Byte]): Boolean = {
    if(message == null){
      // TODO return false here?
      throw new IllegalActorStateException("Received null message in AMQP Remote Server is null")
    }
    else {
      handleRemoteMessageProtocol(RemoteMessageProtocol.parseFrom(message))
      true
    }
  }

  private def handleRemoteMessageProtocol(request: RemoteMessageProtocol) = {
    log.slf4j.debug("Received RemoteMessageProtocol[\n{}]",request)
    request.getActorInfo.getActorType match {
      case SCALA_ACTOR => dispatchToActor(request)
      case TYPED_ACTOR => dispatchToTypedActor(request)
      case JAVA_ACTOR  => throw new IllegalActorStateException("ActorType JAVA_ACTOR is currently not supported")
      case other       => throw new IllegalActorStateException("Unknown ActorType [" + other + "]")
    }
  }

  private def dispatchToActor(request: RemoteMessageProtocol) {

    val actorInfo = request.getActorInfo
    log.slf4j.debug("Dispatching to remote actor [{}:{}]", actorInfo.getTarget, actorInfo.getUuid)

    val actorRef =
      try { createActor(actorInfo, request.getRemoteClientId).start } catch {
        case e: SecurityException =>
          amqpServerBridge.sendMessageTo(createErrorReplyMessage(e, request, AkkaActorType.ScalaActor).toByteArray, Some(request.getRemoteClientId))
          serverModule.notifyListeners(RemoteServerError(e, serverModule))
          return
      }

    val message = MessageSerializer.deserialize(request.getMessage)
    val sender =
      if (request.hasSender) Some(RemoteActorSerialization.fromProtobufToRemoteActorRef(request.getSender, loader))
      else None

    message match { // first match on system messages
      case RemoteActorSystemMessage.Stop =>
        if (UNTRUSTED_MODE) throw new SecurityException("Remote server is operating is untrusted mode, can not stop the actor")
        else actorRef.stop
      case _: LifeCycleMessage if (UNTRUSTED_MODE) =>
        throw new SecurityException("Remote server is operating is untrusted mode, can not pass on a LifeCycleMessage to the remote actor")

      case _ =>     // then match on user defined messages
        if (request.getOneWay) actorRef.!(message)(sender)
        else actorRef.postMessageToMailboxAndCreateFutureResultWithTimeout(
          message,
          request.getActorInfo.getTimeout,
          None,
          Some(new DefaultCompletableFuture[AnyRef](request.getActorInfo.getTimeout).
            onComplete(f => {
              log.slf4j.debug("Future was completed, now flushing to remote!")
              val result = f.result
              val exception = f.exception
              if (exception.isDefined) {
                log.slf4j.debug("Returning exception from actor invocation [{}]",exception.get.getClass)
                amqpServerBridge.sendMessageTo(createErrorReplyMessage(exception.get, request, AkkaActorType.ScalaActor).toByteArray, Some(request.getRemoteClientId))
              }
              else if (result.isDefined) {
                log.slf4j.debug("Returning result from actor invocation [{}]",result.get)
                val messageBuilder:RemoteMessageProtocol.Builder  = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
                  Some(actorRef),
                  Right(request.getUuid),
                  actorInfo.getId,
                  actorInfo.getTarget,
                  actorInfo.getTimeout,
                  Left(result.get),
                  true,
                  Some(actorRef),
                  None,
                  AkkaActorType.ScalaActor,
                  None,
                  None)
                if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
                  amqpServerBridge.sendMessageTo(messageBuilder.build.toByteArray, Some(request.getRemoteClientId))
              }
            }
          )
       ))
    }
  }

  private def dispatchToTypedActor(request: RemoteMessageProtocol) = {
    val actorInfo = request.getActorInfo
    val typedActorInfo = actorInfo.getTypedActorInfo
    log.slf4j.debug("Dispatching to remote typed actor [{} :: {}]", typedActorInfo.getMethod, typedActorInfo.getInterface)

    val typedActor = createTypedActor(actorInfo, request.getRemoteClientId)
    val args = MessageSerializer.deserialize(request.getMessage).asInstanceOf[Array[AnyRef]].toList
    val argClasses = args.map(_.getClass)

    try {
      val messageReceiver = typedActor.getClass.getDeclaredMethod(typedActorInfo.getMethod, argClasses: _*)
      if (request.getOneWay) messageReceiver.invoke(typedActor, args: _*)
      else {
        def sendResponse(result: Either[Any,Throwable]): Unit = try {
          val messageBuilder: RemoteMessageProtocol.Builder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
            None,
            Right(request.getUuid),
            actorInfo.getId,
            actorInfo.getTarget,
            actorInfo.getTimeout,
            result,
            true,
            None,
            None,
            AkkaActorType.TypedActor,
            None,
            None)
          if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
          amqpServerBridge.sendMessageTo(messageBuilder.build.toByteArray, Some(request.getRemoteClientId))
          log.slf4j.debug("Returning result from remote typed actor invocation [{}]", result)
        } catch {
          case e: Throwable => serverModule.notifyListeners(RemoteServerError(e, serverModule))
        }

        messageReceiver.invoke(typedActor, args: _*) match {
          case f: Future[_] => //If it's a future, we can lift on that to defer the send to when the future is completed
            f.onComplete( future => {
              val result: Either[Any,Throwable] =
                if (future.exception.isDefined) Right(future.exception.get) else Left(future.result.get)
              sendResponse(result)
            })
          case other => sendResponse(Left(other))
        }
      }
    } catch {
      case e: InvocationTargetException =>
        amqpServerBridge.sendMessageTo(createErrorReplyMessage(e.getCause, request, AkkaActorType.TypedActor).toByteArray, Some(request.getRemoteClientId))
        serverModule.notifyListeners(RemoteServerError(e, serverModule))
      case e: Throwable =>
        amqpServerBridge.sendMessageTo(createErrorReplyMessage(e, request, AkkaActorType.TypedActor).toByteArray, Some(request.getRemoteClientId))
        serverModule.notifyListeners(RemoteServerError(e, serverModule))
    }
  }

  private def createTypedActor(actorInfo: ActorInfoProtocol, remoteClientId: String): AnyRef = {
    val uuid = actorInfo.getUuid

    serverModule.findTypedActorByIdOrUuid(actorInfo.getId, parseUuid(uuid).toString) match {
      case null => // the actor has not been registered globally. See if we have it in the session
        createTypedSessionActor(actorInfo, remoteClientId) match {
          case null => createClientManagedTypedActor(actorInfo) //Maybe client managed actor?
          case sessionActor => sessionActor
        }
      case typedActor => typedActor
    }
  }

  private def createClientManagedTypedActor(actorInfo: ActorInfoProtocol) = {
      val typedActorInfo = actorInfo.getTypedActorInfo
      val interfaceClassname = typedActorInfo.getInterface
      val targetClassname = actorInfo.getTarget
      val uuid = actorInfo.getUuid

      try {
        if (UNTRUSTED_MODE) throw new SecurityException(
          "Remote server is operating is untrusted mode, can not create remote actors on behalf of the remote client")

        log.slf4j.info("Creating a new remote typed actor:\n\t[{} :: {}]", interfaceClassname, targetClassname)

        val (interfaceClass, targetClass) =
          if (loader.isDefined) (loader.get.loadClass(interfaceClassname),
                                            loader.get.loadClass(targetClassname))
          else (Class.forName(interfaceClassname), Class.forName(targetClassname))

        val newInstance = TypedActor.newInstance(
          interfaceClass, targetClass.asInstanceOf[Class[_ <: TypedActor]], actorInfo.getTimeout).asInstanceOf[AnyRef]
        serverModule.typedActors.put(parseUuid(uuid).toString, newInstance) // register by uuid
        newInstance
      } catch {
        case e =>
          log.slf4j.error("Could not create remote typed actor instance", e)
          serverModule.notifyListeners(RemoteServerError(e, serverModule))
          throw e
      }
    }

  private def createTypedSessionActor(actorInfo: ActorInfoProtocol, remoteClientId: String):AnyRef ={
    val id = actorInfo.getId
    findTypedSessionActor(id, remoteClientId) match {
      case null =>
        serverModule.findTypedActorFactory(id) match {
          case null => null
          case factory =>
            val newInstance = factory()
            typedSessionActors.get(remoteClientId) match{
              case null => typedSessionActors.put(remoteClientId, new ConcurrentHashMap[String, AnyRef])
              case _ =>
            }
            typedSessionActors.get(remoteClientId).put(id, newInstance)
            newInstance
        }
      case sessionActor => sessionActor
    }
  }

  private def findTypedSessionActor(id: String, remoteClientId: String) : AnyRef = {
    typedSessionActors.get(remoteClientId) match {
      case null => null
      case map => map get id
    }
  }

  private def createActor(actorInfo: ActorInfoProtocol, remoteClientId: String): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId

    serverModule.findActorByIdOrUuid(id, parseUuid(uuid).toString) match {
      case null => // the actor has not been registered globally. See if we have it in the session
        createSessionActor(actorInfo, remoteClientId) match {
          case null => createClientManagedActor(actorInfo) // maybe it is a client managed actor
          case sessionActor => sessionActor
        }
      case actorRef => actorRef
    }
  }

  private def createSessionActor(actorInfo: ActorInfoProtocol, remoteClientId: String): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId

    findSessionActor(id, remoteClientId) match {
      case null => // we dont have it in the session either, see if we have a factory for it
        serverModule.findActorFactory(id) match {
          case null => null
          case factory =>
            val actorRef = factory()
            actorRef.uuid = parseUuid(uuid) //FIXME is this sensible?
            sessionActors.get(remoteClientId) match {
              case null => {
                sessionActors.put(remoteClientId, new ConcurrentHashMap[String, ActorRef]())
              }
              case _ =>
            }
            sessionActors.get(remoteClientId).put(id, actorRef)
            actorRef
        }
      case sessionActor => sessionActor
    }
  }

  private def createClientManagedActor(actorInfo: ActorInfoProtocol): ActorRef = {
      val uuid = actorInfo.getUuid
      val id = actorInfo.getId
      val timeout = actorInfo.getTimeout
      val name = actorInfo.getTarget

      try {
        if (UNTRUSTED_MODE) throw new SecurityException(
          "Remote server is operating is untrusted mode, can not create remote actors on behalf of the remote client")

        log.slf4j.info("Creating a new client-managed remote actor [{}:{}]", name, uuid)
        val clazz = if (loader.isDefined) loader.get.loadClass(name)
                    else Class.forName(name)
        val actorRef = Actor.actorOf(clazz.asInstanceOf[Class[_ <: Actor]])
        actorRef.uuid = parseUuid(uuid)
        actorRef.id = id
        actorRef.timeout = timeout
        serverModule.actorsByUuid.put(actorRef.uuid.toString, actorRef) // register by uuid
        actorRef
      } catch {
        case e =>
          log.slf4j.error("Could not create remote actor instance", e)
          serverModule.notifyListeners(RemoteServerError(e, serverModule))
          throw e
      }

    }

  private def findSessionActor(id: String, remoteClientId: String) : ActorRef = {
    sessionActors.get(remoteClientId) match {
      case null => null
      case map => map.get(id)
    }
  }

  def handleRejectedMessage(message: Array[Byte], clientId: String): Unit = {

  }

  protected def parseUuid(protocol: UuidProtocol): Uuid = uuidFrom(protocol.getHigh,protocol.getLow)

  private def createErrorReplyMessage(exception: Throwable, request: RemoteMessageProtocol, actorType: AkkaActorType): RemoteMessageProtocol = {
    val actorInfo = request.getActorInfo
    log.slf4j.error("Could not invoke remote actor [{}]", actorInfo.getTarget)
    log.slf4j.debug("Could not invoke remote actor", exception)
    val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
                          None,
                          Right(request.getUuid),
                          actorInfo.getId,
                          actorInfo.getTarget,
                          actorInfo.getTimeout,
                          Right(exception),
                          true,
                          None,
                          None,
                          actorType,
                          None,
                          if(request.hasRemoteClientId) Some(request.getRemoteClientId) else None)
    if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
    messageBuilder.build
  }
}

