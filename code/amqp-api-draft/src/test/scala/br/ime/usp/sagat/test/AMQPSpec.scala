package br.ime.usp.sagat.test

import org.scalatest.Spec
import org.scalatest.matchers.{ShouldMatchers}
import br.ime.usp.sagat.amqp._


class AMQPSpec extends Spec with ShouldMatchers with DataHelper{
  import AMQPBridge._


  describe("An AMQP server bridge"){
    it("must share the same connection for 2 instances"){
       val connection1 = getRemoteServerConnection
       val connection2 = getRemoteServerConnection
      (connection1 == connection2) should equal (true)
    }

    it("must share the same connection for 3 instances"){
      val connection1 = getRemoteServerConnection
      val connection2 = getRemoteServerConnection
      val connection3 = getRemoteServerConnection
      (connection1 == connection2) should equal (true)
      (connection3 == connection2) should equal (true)
    }

    it("must create the channels in the same connection"){
      val connection1 = getRemoteServerConnection
      val channel1 = connection1.createChannel
      val channel2 = connection1.createChannel

      val connection2 = getRemoteServerConnection
      val channel3 = connection2.createChannel
      val channel4 = connection2.createChannel
      (channel4.getChannelNumber - channel3.getChannelNumber) should be (1)
      (channel3.getChannelNumber - channel2.getChannelNumber) should be (1)
      (channel2.getChannelNumber - channel1.getChannelNumber) should be (1)
    }

    it("must allow neither null names nor null channels"){
      intercept[IllegalArgumentException](
        new AMQPBridgeServer(null, getRemoteServerConnection.createChannel)
      )
      intercept[IllegalArgumentException](
        new AMQPBridgeServer("dummy", null)
      )
    }

    it("must share the same connection in amqp bridge servers"){
      val server1 = new AMQPBridgeServer("dummy", getRemoteServerConnection.createChannel)
      val server2 = new AMQPBridgeServer("dummy2", getRemoteServerConnection.createChannel)
      (server1.channel.getConnection == server2.channel.getConnection) should equal (true)
    }

  }

  describe("An AMQP client bridge"){
    it("must share the same connection for 2 instances"){
       val connection1 = getRemoteClientConnection
       val connection2 = getRemoteClientConnection
      (connection1 == connection2) should equal (true)
    }

    it("must share the same connection for 3 instances"){
      val connection1 = getRemoteClientConnection
      val connection2 = getRemoteClientConnection
      val connection3 = getRemoteClientConnection
      (connection1 == connection2) should equal (true)
      (connection3 == connection2) should equal (true)
    }

    it("must create the channels in the same connection"){
      val connection1 = getRemoteClientConnection
      val channel1 = connection1.createChannel
      val channel2 = connection1.createChannel

      val connection2 = getRemoteClientConnection
      val channel3 = connection2.createChannel
      val channel4 = connection2.createChannel

      (channel4.getChannelNumber - channel3.getChannelNumber) should be (1)
      (channel3.getChannelNumber - channel2.getChannelNumber) should be (1)
      (channel2.getChannelNumber - channel1.getChannelNumber) should be (1)
    }

    it("must not allow neither null names nor null channels"){
      intercept[IllegalArgumentException](
        new AMQPBridgeClient(null, getRemoteClientConnection.createChannel)
      )
      intercept[IllegalArgumentException](
        new AMQPBridgeClient("dummy", null)
      )
    }

    it("must share the same connection in amqp bridge clients"){
      val client1 = new AMQPBridgeClient("dummy", getRemoteClientConnection.createChannel)
      val client2 = new AMQPBridgeClient("dummy2", getRemoteClientConnection.createChannel)
      (client1.channel.getConnection == client2.channel.getConnection) should equal (true)
    }

  }

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
  }

  describe("A Storage Mode"){
    import StorageMode._
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
  }

}