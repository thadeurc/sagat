package br.ime.usp.sagat.test.mock

import br.ime.usp.sagat.amqp.{MessageHandler}
import java.util.concurrent.{CountDownLatch}

class AutoAckMessageConsumer(prefix: String, echo:Boolean = false, latch: CountDownLatch, replyToSender: Boolean = false) extends MessageHandler {

  def process(message: Array[Byte], replyAction: Array[Byte] => Unit): (Boolean, Boolean) = {
    latch.countDown
    val text = new String(message)
    if(echo) println("%s - Consumer received message [%s]" format(prefix, text))
    if(replyToSender){
      replyAction("Resposta da mensagem [%s]".format(text).getBytes)
    }
    (true, false)
  }
}

