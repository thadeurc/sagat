package akka.remote.amqp

import java.util.{Map => JMap}

object ExchangeConfig extends Enumeration {
  val exchangeDurable    = Value("exchangeDurable",typeConfig = "direct", durable = true,  autoDelete = false, arguments = null)
  val exchangeNotDurable = Value("exchangeNotDurable",typeConfig = "direct", durable = false, autoDelete = false, arguments = null)

  class ExchangeParameters(name: String,
                           val typeConfig: String,
                           val durable: Boolean,
                           val autoDelete: Boolean,
                           val arguments: JMap[String, Object]) extends Val(nextId, name)

  protected final def Value(name: String,
                            typeConfig: String,
                            durable: Boolean,
                            autoDelete: Boolean,
                            arguments: JMap[String, Object]): ExchangeParameters =
      new ExchangeParameters(name, typeConfig, durable, autoDelete, arguments)
}

object QueueConfig extends Enumeration {
  val queueDurable    = Value("queueDurable",exclusive = false, durable = true,  autoDelete = false, arguments = null)
  val queueNotDurable = Value("queueNotDurable",exclusive = false, durable = false, autoDelete = false, arguments = null)
  val exclusiveQueue  = Value("exclusiveQueue",exclusive = true, durable = false, autoDelete = true , arguments = null)

  class QueueParameters(name: String,
                        val exclusive: Boolean,
                        val durable: Boolean,
                        val autoDelete: Boolean,
                        val arguments: JMap[String, Object]) extends Val(nextId, name)

  protected final def Value(name: String,
                            exclusive: Boolean,
                            durable: Boolean,
                            autoDelete: Boolean,
                            arguments: JMap[String, Object]): QueueParameters =
      new QueueParameters(name, exclusive, durable, autoDelete, arguments)
}

object StorageAndConsumptionPolicy extends Enumeration {
  import ExchangeConfig._
  import QueueConfig._

  val NOT_DURABLE  = Value("NOT_DURABLE", exchangeNotDurable, queueNotDurable)
  val DURABLE = Value("DURABLE", exchangeDurable, queueDurable)
  val EXCLUSIVE_AUTODELETE  = Value("EXCLUSIVE_AUTODELETE",exchangeNotDurable, exclusiveQueue)

  class MessageStorageAndConsumptionPolicyParams(name: String,
                                                 val exchangeParams: ExchangeParameters,
                                                 val queueParams: QueueParameters) extends Val(nextId, name)

  protected final def Value(name: String,
                            exchangeParams: ExchangeParameters,
                            queueParams: QueueParameters): MessageStorageAndConsumptionPolicyParams =
      new MessageStorageAndConsumptionPolicyParams(name, exchangeParams, queueParams)
}

