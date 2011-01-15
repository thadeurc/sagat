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
    def fanout = typeConfig eq "fanout"
  }
  val exchangeDurable    = ExchangeParameters(typeConfig = "direct", durable = true,  autoDelete = false, arguments = null, 1)
  val exchangeAutoDelete = ExchangeParameters(typeConfig = "direct", durable = false, autoDelete = true,  arguments = null, 2)
  val exchangeNotDurable = ExchangeParameters(typeConfig = "direct", durable = false, autoDelete = false, arguments = null, 3)
  val exchangeNotDurableAutoDelete = ExchangeParameters(typeConfig = "direct", durable = false, autoDelete = true, arguments = null, 4)
  val fanoutExchangeDurable    = ExchangeParameters(typeConfig = "fanout", durable = true,  autoDelete = false, arguments = null, 5)
  val fanoutExchangeAutoDelete = ExchangeParameters(typeConfig = "fanout", durable = false, autoDelete = true,  arguments = null, 6)
  val fanoutExchangeNotDurable = ExchangeParameters(typeConfig = "fanout", durable = false, autoDelete = false, arguments = null, 7)
  val fanoutExchangeNotDurableAutoDelete = ExchangeParameters(typeConfig = "fanout", durable = false, autoDelete = true, arguments = null, 8)
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
  val exclusiveQueueDurable    = QueueParameters(exclusive = true, durable = true,  autoDelete = false, arguments = null, 5)
  val exclusiveQueueAutoDelete = QueueParameters(exclusive = true, durable = false, autoDelete = true , arguments = null, 6)
  val exclusiveQueueNotDurable = QueueParameters(exclusive = true, durable = false, autoDelete = false, arguments = null, 7)
  val exclusiveQueueNotDurableAutoDelete = QueueParameters(exclusive = true, durable = false, autoDelete = true, arguments = null, 8)

}

object StorageAndConsumptionPolicy extends Enumeration {
  import ExchangeConfig._
  import QueueConfig._

  type MessageStorageAndConsumptionPolicy = MessageStorageAndConsumptionPolicyParams

  case class MessageStorageAndConsumptionPolicyParams(exchangeParams: ExchangeParameters,
                                                      queueParams: QueueParameters,
                                                      enumId: Int) extends Value{
    override def id = this.enumId
  }

  val TRANSIENT  = MessageStorageAndConsumptionPolicyParams(exchangeNotDurable, queueNotDurable, 1)
  val PERSISTENT = MessageStorageAndConsumptionPolicyParams(exchangeDurable, queueDurable      , 2)
  val TRANSIENT_AUTODELETE = MessageStorageAndConsumptionPolicyParams(exchangeNotDurableAutoDelete, queueNotDurableAutoDelete, 3)
  val EXCLUSIVE_TRANSIENT  = MessageStorageAndConsumptionPolicyParams(fanoutExchangeNotDurable, exclusiveQueueNotDurable, 4)
  val EXCLUSIVE_PERSISTENT = MessageStorageAndConsumptionPolicyParams(fanoutExchangeDurable, exclusiveQueueDurable      , 5)
  val EXCLUSIVE_TRANSIENT_AUTODELETE = MessageStorageAndConsumptionPolicyParams(fanoutExchangeNotDurableAutoDelete, exclusiveQueueNotDurableAutoDelete, 6)


}

