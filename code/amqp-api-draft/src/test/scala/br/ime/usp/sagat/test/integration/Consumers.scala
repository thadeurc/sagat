package br.ime.usp.sagat.test.mock

import br.ime.usp.sagat.amqp.{MessageHandler}
import java.util.concurrent.CountDownLatch


class AutoAckMessageConsumer(prefix: String, echo:Boolean = false, latch: CountDownLatch) extends MessageHandler {

  def process(message: Array[Byte]): (Boolean, Boolean) = {
    latch.countDown
    if(echo) println("%s - Consumer received message [%s]" format(prefix, new String(message)))
    (true, false)
  }
}

