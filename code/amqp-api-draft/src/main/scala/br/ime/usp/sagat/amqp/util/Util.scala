package br.ime.usp.sagat.amqp.util

import com.rabbitmq.client.{Connection, Channel}

trait Logging {
 @transient val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)
}

trait ControlStructures extends Logging {
  def withOpenChannelOrException(channel: Channel)(action: => Unit){
    if(channel.isOpen){
      action
    }
    else throw new IllegalStateException("Channel is closed")
  }

  def silentClose(connection: Connection){
    require(connection != null)
    if(connection.isOpen){
      try{
        connection.close
        log.info("Closed connection ref: {}", connection)
      }catch{
        case e: Throwable => log.warn("Exception closing connection ref {}", connection, e)
      }
    }else{
      log.warn("Connection ref {} already closed", connection, new RuntimeException)
    }
  }
}