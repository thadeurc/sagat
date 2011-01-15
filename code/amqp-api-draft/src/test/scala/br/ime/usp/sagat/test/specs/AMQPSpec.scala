package br.ime.usp.sagat.test

import org.scalatest.Spec
import org.scalatest.matchers.{ShouldMatchers}
import br.ime.usp.sagat.amqp._



class AMQPSpec extends Spec with ShouldMatchers {

  describe("An exchange configuration"){
    import ExchangeConfig._
    it("must provide a reliable DURABLE configuration "){
      val durable = exchangeDurable
      ("direct".equals(durable.typeConfig)) should equal (true)
      durable.autoDelete should equal (false)
      durable.durable should equal (true)
      durable.id should be (1)
    }

    it("must provide a reliable AUTODELETE configuration "){
      val autoDelete = exchangeAutoDelete
      ("direct".equals(autoDelete.typeConfig)) should equal (true)
      autoDelete.autoDelete should equal (true)
      autoDelete.durable should equal (false)
      autoDelete.id should be (2)
    }

    it("must provide a reliable NOTDURABLE configuration "){
      val notDurable = exchangeNotDurable
      ("direct".equals(notDurable.typeConfig)) should equal (true)
      notDurable.autoDelete should equal (false)
      notDurable.durable should equal (false)
      notDurable.id should be (3)
    }

    it("must provide a reliable NOTDURABLEAUTODELETE configuration "){
      val notDurableAutoDelete = exchangeNotDurableAutoDelete
      ("direct".equals(notDurableAutoDelete.typeConfig)) should equal (true)
      notDurableAutoDelete.autoDelete should equal (true)
      notDurableAutoDelete.durable should equal (false)
      notDurableAutoDelete.id should be (4)
    }

    it("must provide a reliable FANOUT DURABLE configuration "){
      val durable = fanoutExchangeDurable
      ("fanout".equals(durable.typeConfig)) should equal (true)
      durable.autoDelete should equal (false)
      durable.durable should equal (true)
      durable.id should be (5)
    }

    it("must provide a reliable FANOUT AUTODELETE configuration "){
      val autoDelete = fanoutExchangeAutoDelete
      ("fanout".equals(autoDelete.typeConfig)) should equal (true)
      autoDelete.autoDelete should equal (true)
      autoDelete.durable should equal (false)
      autoDelete.id should be (6)
    }

    it("must provide a reliable FANOUT NOTDURABLE configuration "){
      val notDurable = fanoutExchangeNotDurable
      ("fanout".equals(notDurable.typeConfig)) should equal (true)
      notDurable.autoDelete should equal (false)
      notDurable.durable should equal (false)
      notDurable.id should be (7)
    }

    it("must provide a reliable FANOUT NOTDURABLEAUTODELETE configuration "){
      val notDurableAutoDelete = fanoutExchangeNotDurableAutoDelete
      ("fanout".equals(notDurableAutoDelete.typeConfig)) should equal (true)
      notDurableAutoDelete.autoDelete should equal (true)
      notDurableAutoDelete.durable should equal (false)
      notDurableAutoDelete.id should be (8)
    }


  }

  describe("A Queue configuration"){
    import QueueConfig._
    it("must provide a reliable DURABLE configuration "){
      val durable = queueDurable
      durable.exclusive should equal (false)
      durable.autoDelete should equal (false)
      durable.durable should equal (true)
      durable.id should be (1)
    }

    it("must provide a reliable AUTODELETE configuration "){
      val autoDelete = queueAutoDelete
      autoDelete.exclusive should equal (false)
      autoDelete.autoDelete should equal (true)
      autoDelete.durable should equal (false)
      autoDelete.id should be (2)
    }

    it("must provide a reliable NOTDURABLE configuration "){
      val notDurable = queueNotDurable
      notDurable.exclusive should equal (false)
      notDurable.autoDelete should equal (false)
      notDurable.durable should equal (false)
      notDurable.id should be (3)
    }

    it("must provide a reliable NOTDURABLEAUTODELETE configuration "){
      val notDurableAutoDelete = queueNotDurableAutoDelete
      notDurableAutoDelete.exclusive should equal (false)
      notDurableAutoDelete.autoDelete should equal (true)
      notDurableAutoDelete.durable should equal (false)
      notDurableAutoDelete.id should be (4)
    }

    it("must provide a reliable EXCLUSIVE DURABLE configuration "){
      val durable = exclusiveQueueDurable
      durable.exclusive should equal (true)
      durable.autoDelete should equal (false)
      durable.durable should equal (true)
      durable.id should be (5)
    }

    it("must provide a reliable EXCLUSIVE AUTODELETE configuration "){
      val autoDelete = exclusiveQueueAutoDelete
      autoDelete.exclusive should equal (true)
      autoDelete.autoDelete should equal (true)
      autoDelete.durable should equal (false)
      autoDelete.id should be (6)
    }

    it("must provide a reliable EXCLUSIVE NOTDURABLE configuration "){
      val notDurable = exclusiveQueueNotDurable
      notDurable.exclusive should equal (true)
      notDurable.autoDelete should equal (false)
      notDurable.durable should equal (false)
      notDurable.id should be (7)
    }

    it("must provide a reliable EXCLUSIVE NOTDURABLEAUTODELETE configuration "){
      val notDurableAutoDelete = exclusiveQueueNotDurableAutoDelete
      notDurableAutoDelete.exclusive should equal (true)
      notDurableAutoDelete.autoDelete should equal (true)
      notDurableAutoDelete.durable should equal (false)
      notDurableAutoDelete.id should be (8)
    }
  }

  describe("A Storage Policy"){
    import StorageAndConsumptionPolicy._
    import ExchangeConfig._
    import QueueConfig._

    it("must provide a reliable TRANSIENT configuration"){
      val transient = TRANSIENT
      (transient.exchangeParams == exchangeNotDurable) should equal (true)
      (transient.queueParams == queueNotDurable) should equal (true)
      transient.id should be (1)
    }

    it("must provide a reliable PERSISTENT configuration"){
      val persistent = PERSISTENT
      (persistent.exchangeParams == exchangeDurable) should equal (true)
      (persistent.queueParams == queueDurable) should equal (true)
      persistent.id should be (2)
    }

    it("must provide a reliable TRANSIENT_AUTODELETE configuration"){
      val transientAutoclean = TRANSIENT_AUTODELETE
      (transientAutoclean.exchangeParams == exchangeNotDurableAutoDelete) should equal (true)
      (transientAutoclean.queueParams == queueNotDurableAutoDelete) should equal (true)
      transientAutoclean.id should be (3)
    }

     it("must provide a reliable EXCLUSIVE TRANSIENT configuration"){
      val transient = EXCLUSIVE_TRANSIENT
      (transient.exchangeParams == fanoutExchangeNotDurable) should equal (true)
      (transient.queueParams == exclusiveQueueNotDurable) should equal (true)
      transient.id should be (4)
    }

    it("must provide a reliable EXCLUSIVE PERSISTENT configuration"){
      val persistent = EXCLUSIVE_PERSISTENT
      (persistent.exchangeParams == fanoutExchangeDurable) should equal (true)
      (persistent.queueParams == exclusiveQueueDurable) should equal (true)
      persistent.id should be (5)
    }

    it("must provide a reliable EXCLUSIVE TRANSIENT_AUTODELETE configuration"){
      val transientAutoclean = EXCLUSIVE_TRANSIENT_AUTODELETE
      (transientAutoclean.exchangeParams == fanoutExchangeNotDurableAutoDelete) should equal (true)
      (transientAutoclean.queueParams == exclusiveQueueNotDurableAutoDelete) should equal (true)
      transientAutoclean.id should be (6)
    }
  }
}