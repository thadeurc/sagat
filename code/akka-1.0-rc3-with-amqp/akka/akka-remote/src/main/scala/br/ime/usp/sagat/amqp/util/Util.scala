package br.ime.usp.sagat.amqp.util

import br.ime.usp.sagat.amqp.AMQPBridge


trait Logging {
 @transient val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)
}

trait ControlStructures {
  def ifTrueOrException(statement: Boolean)(body: => Unit) {
    if(statement) body
    else throw new IllegalStateException("Not valid state to execute.")

  }
}

