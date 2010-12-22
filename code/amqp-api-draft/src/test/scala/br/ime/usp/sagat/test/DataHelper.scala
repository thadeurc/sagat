package br.ime.usp.sagat.test

import java.lang.String
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope, ShutdownSignalException, Consumer, DefaultConsumer}
import scala.collection.mutable.Set

trait DataHelper {
    lazy val defaultFakeConsumer = new DefaultConsumer(null)

    lazy val messagesToSend = for(i <- 1 to 10) yield "msgxxx x x x x "+i

    lazy val defaultConsumer = new MyDefaultConsumer
}

class MyDefaultConsumer extends Consumer with DataHelper{

  private val received = Set[String]()

  def difference = {
    messagesToSend.toSet -- (received)

  }

  def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) = {
    received.add(new String(body))
  }

  def handleRecoverOk = {}

  def handleShutdownSignal(p1: String, p2: ShutdownSignalException) = {}

  def handleCancelOk(p1: String) = {}

  def handleConsumeOk(p1: String) = {}
}