package akka.remote.amqp

import java.util.{Map => JMap}

object ExchangeConfig extends Enumeration {
  val exchangeDurable    = Value("exchangeDurable",typeConfig = "direct", durable = true,  autoDelete = false, arguments = null)
  val exchangeAutoDelete = Value("exchangeAutoDelete",typeConfig = "direct", durable = false, autoDelete = true,  arguments = null)
  val exchangeNotDurable = Value("exchangeNotDurable",typeConfig = "direct", durable = false, autoDelete = false, arguments = null)
  val exchangeNotDurableAutoDelete = Value("exchangeNotDurableAutoDelete",typeConfig = "direct", durable = false, autoDelete = true, arguments = null)
  val fanoutExchangeDurable    = Value("fanoutExchangeDurable",typeConfig = "fanout", durable = true,  autoDelete = false, arguments = null)
  val fanoutExchangeAutoDelete = Value("fanoutExchangeAutoDelete",typeConfig = "fanout", durable = false, autoDelete = true,  arguments = null)
  val fanoutExchangeNotDurable = Value("fanoutExchangeNotDurable",typeConfig = "fanout", durable = false, autoDelete = false, arguments = null)
  val fanoutExchangeNotDurableAutoDelete = Value("fanoutExchangeNotDurableAutoDelete",typeConfig = "fanout", durable = false, autoDelete = true, arguments = null)

  class ExchangeParameters(name: String,
                           val typeConfig: String,
                           val durable: Boolean,
                           val autoDelete: Boolean,
                           val arguments: JMap[String, Object]) extends Val(nextId, name) {
      def fanout = typeConfig eq "fanout"
  }
  protected final def Value(name: String,
                            typeConfig: String,
                            durable: Boolean,
                            autoDelete: Boolean,
                            arguments: JMap[String, Object]): ExchangeParameters =
      new ExchangeParameters(name, typeConfig, durable, autoDelete, arguments)
}

object QueueConfig extends Enumeration {
  val queueDurable    = Value("queueDurable",exclusive = false, durable = true,  autoDelete = false, arguments = null)
  val queueAutoDelete = Value("queueAutoDelete",exclusive = false, durable = false, autoDelete = true , arguments = null)
  val queueNotDurable = Value("queueNotDurable",exclusive = false, durable = false, autoDelete = false, arguments = null)
  val queueNotDurableAutoDelete = Value("queueNotDurableAutoDelete",exclusive = false, durable = false, autoDelete = true, arguments = null)
  val exclusiveQueueDurable    = Value("exclusiveQueueDurable",exclusive = true, durable = true,  autoDelete = false, arguments = null)
  val exclusiveQueueAutoDelete = Value("exclusiveQueueAutoDelete",exclusive = true, durable = false, autoDelete = true , arguments = null)
  val exclusiveQueueNotDurable = Value("exclusiveQueueNotDurable",exclusive = true, durable = false, autoDelete = false, arguments = null)
  val exclusiveQueueNotDurableAutoDelete = Value("exclusiveQueueNotDurableAutoDelete",exclusive = true, durable = false, autoDelete = true, arguments = null)

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

  val TRANSIENT  = Value("TRANSIENT", exchangeNotDurable, queueNotDurable)
  val PERSISTENT = Value("PERSISTENT", exchangeDurable, queueDurable)
  val TRANSIENT_AUTODELETE = Value("TRANSIENT_AUTODELETE", exchangeNotDurableAutoDelete, queueNotDurableAutoDelete)
  val EXCLUSIVE_TRANSIENT  = Value("EXCLUSIVE_TRANSIENT",exchangeNotDurable, exclusiveQueueNotDurable)
  val EXCLUSIVE_PERSISTENT = Value("EXCLUSIVE_PERSISTENT",exchangeDurable, exclusiveQueueDurable)
  val EXCLUSIVE_TRANSIENT_AUTODELETE = Value("EXCLUSIVE_TRANSIENT_AUTODELETE",exchangeNotDurableAutoDelete, exclusiveQueueNotDurableAutoDelete)
  val EXCLUSIVE_FANOUT_TRANSIENT  = Value("EXCLUSIVE_FANOUT_TRANSIENT",fanoutExchangeNotDurable, exclusiveQueueNotDurable)
  val EXCLUSIVE_FANOUT_PERSISTENT = Value("EXCLUSIVE_FANOUT_PERSISTENT",fanoutExchangeDurable, exclusiveQueueDurable)
  val EXCLUSIVE_FANOUT_TRANSIENT_AUTODELETE = Value("EXCLUSIVE_FANOUT_TRANSIENT_AUTODELETE",fanoutExchangeNotDurableAutoDelete, exclusiveQueueNotDurableAutoDelete)

  class MessageStorageAndConsumptionPolicyParams(name: String,
                                                 val exchangeParams: ExchangeParameters,
                                                 val queueParams: QueueParameters) extends Val(nextId, name){
    def fanout: Boolean = exchangeParams.fanout
  }
  protected final def Value(name: String,
                            exchangeParams: ExchangeParameters,
                            queueParams: QueueParameters): MessageStorageAndConsumptionPolicyParams =
      new MessageStorageAndConsumptionPolicyParams(name, exchangeParams, queueParams)
}

