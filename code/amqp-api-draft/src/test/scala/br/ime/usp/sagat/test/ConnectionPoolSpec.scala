package br.ime.usp.sagat.test

import org.easymock.EasyMock
import com.rabbitmq.client.Connection
import br.ime.usp.sagat.amqp.util.{ConnectionSharePolicy, ReadAndWriteConnectionFactory, ReadOrWriteConnectionFactory, ConnectionPoolDefinition}
import ConnectionSharePolicy._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.Spec

class ConnectionPoolSpec extends Spec with ShouldMatchers {

  describe("A ConnectionPool"){
    it("SERVER - Should return the same connection for different get connection requests for the same nodeName - One Conn Per Channel"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_CHANNEL)
      val c2 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_CHANNEL)
      c1 should equal (c2)
    }

    it("SERVER - Should return the same connection for different get connection requests for the same nodeName - One Conn Per Node"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_NODE)
      val c2 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_NODE)
      c1 should equal (c2)
    }

    it("SERVER - Should ensure that if a different policy is used to ask for an existing connection an exception is thrown"){
      val env = new ConnectionPoolTestEnvironment
      env.getConnectionForServerBridge("name1", ONE_CONN_PER_CHANNEL)
      intercept[IllegalArgumentException]{
        env.getConnectionForServerBridge("name1", ONE_CONN_PER_NODE)
      }
    }

    it("SERVER - Should return a different connection for a different node - One conn per Channel"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_CHANNEL)
      val c2 = env.getConnectionForServerBridge("name2", ONE_CONN_PER_CHANNEL)
      c1 should not equal (c2)
    }

    it("SERVER - Should return a different connection for a different node - One conn per node"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_NODE)
      val c2 = env.getConnectionForServerBridge("name2", ONE_CONN_PER_NODE)
      c1 should not equal (c2)
    }

    it("SERVER - Should return a different connection for a different node no mater the share policy"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForServerBridge("name1", ONE_CONN_PER_CHANNEL)
      val c2 = env.getConnectionForServerBridge("name2", ONE_CONN_PER_NODE)
      c1 should not equal (c2)
    }


    it("CLIENT - Should return the same connection for different get connection requests for the same nodeName - One conn per Channel"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_CHANNEL)
      val c2 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_CHANNEL)
      c1 should equal (c2)
    }

    it("CLIENT - Should return the same connection for different get connection requests for the same nodeName - One conn per Node"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_NODE)
      val c2 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_NODE)
      c1 should equal (c2)
    }

    it("CLIENT - Should ensure that if a different policy is used to ask for an existing connection an exception is thrown"){
      val env = new ConnectionPoolTestEnvironment
      env.getConnectionForClientBridge("name1", ONE_CONN_PER_CHANNEL)
      intercept[Exception]{
        env.getConnectionForClientBridge("name1", ONE_CONN_PER_NODE)
      }
    }

    it("CLIENT - Should return a different connection for a different node - One conn per channel"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_CHANNEL)
      val c2 = env.getConnectionForClientBridge("name2", ONE_CONN_PER_CHANNEL)
      c1 should not equal (c2)
    }

    it("CLIENT - Should return a different connection for a different node - one conn per node"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_NODE)
      val c2 = env.getConnectionForClientBridge("name2", ONE_CONN_PER_NODE)
      c1 should not equal (c2)
    }

    it("CLIENT - Should return a different connection for a different node no mater the share policy"){
      val env = new ConnectionPoolTestEnvironment
      val c1 = env.getConnectionForClientBridge("name1", ONE_CONN_PER_CHANNEL)
      val c2 = env.getConnectionForClientBridge("name2", ONE_CONN_PER_NODE)
      c1 should not equal (c2)
    }

  }

}

class ConnectionPoolTestEnvironment extends ConnectionPoolDefinition  {
  import EasyMock._
  override val readOrWriteConnFactory:ReadOrWriteConnectionFactory  = createNiceMock(classOf[ReadOrWriteConnectionFactory])
  override val readAndWriteConnFactory:ReadAndWriteConnectionFactory  = createNiceMock(classOf[ReadAndWriteConnectionFactory])

  val connectionReadOrWriteMock: Connection = createNiceMock(classOf[Connection])
  val connectionReadOrWriteMock2: Connection = createNiceMock(classOf[Connection])
  val connectionReadOrWriteMock3: Connection = createNiceMock(classOf[Connection])
  val connectionReadOrWriteMock4: Connection = createNiceMock(classOf[Connection])

  val connectionReadAndWriteMock: Connection = createNiceMock(classOf[Connection])
  val connectionReadAndWriteMock1: Connection = createNiceMock(classOf[Connection])
  val connectionReadAndWriteMock2: Connection = createNiceMock(classOf[Connection])

  expect(readOrWriteConnFactory.newConnection).andReturn(connectionReadOrWriteMock)
  expect(readOrWriteConnFactory.newConnection).andReturn(connectionReadOrWriteMock2)
  expect(readOrWriteConnFactory.newConnection).andReturn(connectionReadOrWriteMock3)
  expect(readOrWriteConnFactory.newConnection).andReturn(connectionReadOrWriteMock4)

  expect(readAndWriteConnFactory.newConnection).andReturn(connectionReadAndWriteMock)
  expect(readAndWriteConnFactory.newConnection).andReturn(connectionReadAndWriteMock1)
  expect(readAndWriteConnFactory.newConnection).andReturn(connectionReadAndWriteMock2)

  expect(connectionReadOrWriteMock.getChannelMax).andReturn(ONE_CONN_PER_CHANNEL.channels).anyTimes
  expect(connectionReadOrWriteMock.isOpen).andReturn(true).anyTimes
  expect(connectionReadOrWriteMock2.getChannelMax).andReturn(ONE_CONN_PER_CHANNEL.channels).anyTimes
  expect(connectionReadOrWriteMock2.isOpen).andReturn(true).anyTimes
  expect(connectionReadOrWriteMock3.getChannelMax).andReturn(ONE_CONN_PER_CHANNEL.channels).anyTimes
  expect(connectionReadOrWriteMock3.isOpen).andReturn(true).anyTimes
  expect(connectionReadOrWriteMock4.getChannelMax).andReturn(ONE_CONN_PER_CHANNEL.channels).anyTimes
  expect(connectionReadOrWriteMock4.isOpen).andReturn(true).anyTimes

  expect(connectionReadAndWriteMock.getChannelMax).andReturn(ONE_CONN_PER_NODE.channels).anyTimes
  expect(connectionReadAndWriteMock.isOpen).andReturn(true).anyTimes

  expect(connectionReadAndWriteMock1.getChannelMax).andReturn(ONE_CONN_PER_NODE.channels).anyTimes
  expect(connectionReadAndWriteMock1.isOpen).andReturn(true).anyTimes

  expect(connectionReadAndWriteMock2.getChannelMax).andReturn(ONE_CONN_PER_NODE.channels).anyTimes
  expect(connectionReadAndWriteMock2.isOpen).andReturn(true).anyTimes

  replay(readOrWriteConnFactory)
  replay(readAndWriteConnFactory)
  replay(connectionReadAndWriteMock)
  replay(connectionReadAndWriteMock1)
  replay(connectionReadAndWriteMock2)
  replay(connectionReadOrWriteMock)
  replay(connectionReadOrWriteMock2)
  replay(connectionReadOrWriteMock3)
  replay(connectionReadOrWriteMock4)

}

