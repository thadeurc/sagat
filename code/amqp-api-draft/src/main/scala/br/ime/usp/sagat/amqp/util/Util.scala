package br.ime.usp.sagat.amqp.util

import com.rabbitmq.client.Channel

trait Logging {
 @transient val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)
}

trait ControlStructures {
  def withOpenChannel(channel: Channel)(action: => Unit){
    if(channel.isOpen){
      action
    }
    else throw new IllegalStateException("Channel is closed")
  }
}