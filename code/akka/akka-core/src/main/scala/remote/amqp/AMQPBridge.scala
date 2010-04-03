package se.scalablesolutions.akka.remote.amqp

import se.scalablesolutions.akka.util.Logging
import net.lag.configgy.Config    
import com.rabbitmq.client.{Connection, ConnectionFactory, ConnectionParameters}

object AMQPBridge {

  private var bridge: AMQPBridge = null; 

  def getAMQPBridge(conf: Config): AMQPBridge = {
    if(bridge == null){
      bridge = new AMQPBridge(conf)
      bridge.boot
      bridge.start
    }

    bridge
  }
}

class AMQPBridge(conf: Config) extends Logging {
  // TODO create properties in akka.conf
  private val HOSTNAME = conf.getString("akka.remote.amqp.server.hostname", "localhost")
  private val PORT = conf.getInt("akka.remote.amqp.server.port", 5672)
  private val USER = conf.getString("akka.remote.amqp.username","actorDispMan")
  private val PASSWORD = conf.getString("akka.remote.amqp.password","act0r61spMan")
  private val VIRTUAL_HOST = conf.getString("akka.remote.amqp.virtualhost","/actorHost")
  @transient private var _started = false;
  @transient private var _booted = false;

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

  private def boot: Unit = {
    if(!_booted){
      log.info("Booting AMQP Bootstrap")
      log.info("Boot Completed!")
      _booted = true
    }
  }

  private def start: Unit = {
    if(!_started){
      log.info("Starting AMQP Bootstrap")
      log.info("Done!")
      _started = true
    }
  }


  def connection: Connection = {
    log.info("Creating connection to [%s:%s]",HOSTNAME,PORT)
    factory.newConnection(HOSTNAME, PORT)
  }

  def releaseExternalResources: Unit = {
    // TODO make usage of listeners to figure out when disconneted - should be in setupChannel  - as it is a single
    // point might not be necessary
  }
}