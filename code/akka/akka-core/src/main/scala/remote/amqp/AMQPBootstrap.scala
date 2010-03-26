package se.scalablesolutions.akka.remote.amqp

import se.scalablesolutions.akka.util.Logging
import net.lag.configgy.Config    
import com.rabbitmq.client.{Connection, Channel, ConnectionFactory, ConnectionParameters}

class AMQPBootstrap(conf: Config) extends Logging {
  // TODO create properties in akka.conf
  private val HOSTNAME = conf.getString("akka.remote.amqp.server.hostname", "localhost")
  private val PORT = conf.getInt("akka.remote.amqp.server.port", 5672)
  private val USER = conf.getString("akka.remote.amqp.username","actorDispMan")
  private val PASSWORD = conf.getString("akka.remote.amqp.password","act0r61spMan")
  private val VIRTUAL_HOST = conf.getString("akka.remote.amqp.virtualhost","/actorHost")  
  private val EXCHANGE_NAME = conf.getString("akka.remote.ampq.exchange.name","ActorExchange")
  private val EXCHANGE_TYPE = conf.getString("akka.remote.ampq.exchange.type","direct")
  private val QUEUE_NAME = conf.getString("akka.remote.ampq.queue.name","actorQueue")
  private val QUEUE_BIND = conf.getString("akka.remote.ampq.queue.bind","default")

  private val params = {                          
    val config = new ConnectionParameters
    config setUsername USER
    config setPassword PASSWORD
    config setVirtualHost VIRTUAL_HOST
    config setRequestedHeartbeat 0
    config
  }

  private val factory = {
    new ConnectionFactory(params)
  }

  private var channel: Channel = null

  def boot: Unit = {
    log.info("Booting AMQP Bootstrap")
    channel = setupChannel(connection)
    log.info("Boot Completed!")
  }

  def start: Unit = {
    log.info("Starting AMQP Bootstrap")
    log.info("Done!")
  }

  private def setupChannel(conn: Connection): Channel = {
    // TODO echo to all values
    log.info("Setting up Channel to Queue: "+QUEUE_NAME)
    val channel: Channel = conn.createChannel()
    channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE)
    channel.queueDeclare(QUEUE_NAME)
    channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, QUEUE_BIND)
    log.info("Done!")
    channel
  }

  private def connection: Connection = {
    log.info("Creating connection to "+HOSTNAME+":"+PORT)
    factory.newConnection(HOSTNAME, PORT)
  }

  def releaseExternalResources: Unit = {
    // TODO make usage of listeners to figure out when disconneted - should be in setupChannel
    val connection = channel.getConnection()
    channel close()
    connection close()    
  }
}