package br.ime.usp.sagat.amqp

import java.util.{Map => JMap}

object ExchangeConfig extends Enumeration {
  type ExchangeConfig = ExchangeParameters
  case class ExchangeParameters(typeConfig: String,
                                durable: Boolean,
                                autoDelete: Boolean,
                                arguments: JMap[String, Object],
                                enumId: Int) extends Value {
    override def id = this.enumId
  }
  val exchangeDurable    = ExchangeParameters(typeConfig = "direct", durable = true,  autoDelete = false, arguments = null, 1)
  val exchangeAutoDelete = ExchangeParameters(typeConfig = "direct", durable = false, autoDelete = true,  arguments = null, 2)
  val exchangeNotDurable = ExchangeParameters(typeConfig = "direct", durable = false, autoDelete = false, arguments = null, 3)
  val exchangeNotDurableAutoDelete = ExchangeParameters(typeConfig = "direct", durable = false, autoDelete = true, arguments = null, 4)
}

object QueueConfig extends Enumeration {
 type QueueConfig = QueueParameters

  case class QueueParameters(exclusive: Boolean,
                             durable: Boolean,
                             autoDelete: Boolean,
                             arguments: JMap[String, Object],
                             enumId: Int) extends Value {
    override def id = this.enumId
  }

  val queueDurable    = QueueParameters(exclusive = false, durable = true,  autoDelete = false, arguments = null, 1)
  val queueAutoDelete = QueueParameters(exclusive = false, durable = false, autoDelete = true , arguments = null, 2)
  val queueNotDurable = QueueParameters(exclusive = false, durable = false, autoDelete = false, arguments = null, 3)
  val queueNotDurableAutoDelete = QueueParameters(exclusive = false, durable = false, autoDelete = true, arguments = null, 4)
}

object StoragePolicy extends Enumeration {
  import ExchangeConfig._
  import QueueConfig._

  type MessageStorePolicy = MessageStorePolicyParams

  case class MessageStorePolicyParams(exchangeParams: ExchangeParameters,
                                    queueParams: QueueParameters,
                                    enumId: Int) extends Value{
    override def id = this.enumId
  }

  val TRANSIENT  = MessageStorePolicyParams(exchangeNotDurable, queueNotDurable, 1)
  val PERSISTENT = MessageStorePolicyParams(exchangeDurable, queueDurable      , 2)
  val TRANSIENT_AUTODELETE = MessageStorePolicyParams(exchangeNotDurableAutoDelete, queueNotDurableAutoDelete, 3)

}

