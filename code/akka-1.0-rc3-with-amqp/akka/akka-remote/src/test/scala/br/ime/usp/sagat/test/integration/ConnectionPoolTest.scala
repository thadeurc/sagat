package br.ime.usp.sagat.amqp.test.integration

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import br.ime.usp.sagat.amqp.util.AMQPConnectionFactory._
import br.ime.usp.sagat.amqp.util.ConnectionSharePolicy._
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ConnectionPoolTest extends JUnitSuite with ShouldMatchers {
  import TimeUnit._

  @Test
  def connectionHasSupervisor = {
    val supervisedConn = newSupervisedConnection("mynode", ONE_CONN_PER_NODE)
    supervisedConn.isRunning should equal(true)
    supervisedConn.supervisor.isDefined should equal(true)
    supervisedConn.stop
  }

  @Test
  def readChannelHasSupervisor = {
    val latch = new CountDownLatch(1)
    val supervisedConn = newSupervisedConnection("mynode", ONE_CONN_PER_NODE)
    val readChannel = newSupervisedReadChannel(supervisedConn)
    readChannel.isRunning should equal(true)
    readChannel.supervisor.isDefined should equal(true)
    readChannel.supervisor.get should equal(supervisedConn)
    latch.await(1, SECONDS)
    supervisedConn.stop

  }

  @Test
  def writeChannelHasSupervisor = {
    val latch = new CountDownLatch(1)
    val supervisedConn = newSupervisedConnection("mynode", ONE_CONN_PER_NODE)
    val writeChannel = newSupervisedWriteChannel(supervisedConn)
    writeChannel.isRunning should equal(true)
    writeChannel.supervisor.isDefined should equal(true)
    writeChannel.supervisor.get should equal(supervisedConn)
    latch.await(1, SECONDS)
    supervisedConn.stop

  }

  @Test
  def shutdownOfAllSupervisionedConnections = {
    val latch = new CountDownLatch(1)
    val c1 = newSupervisedConnection("mynode1", ONE_CONN_PER_NODE)
    val c2 = newSupervisedConnection("mynode2", ONE_CONN_PER_NODE)
    val c3 = newSupervisedConnection("mynode3", ONE_CONN_PER_NODE)
    val c4 = newSupervisedConnection("mynode4", ONE_CONN_PER_NODE)
    latch.await(1, SECONDS)
    shutdownAll
    latch.await(1, SECONDS)
    linkedCount should be (0)
  }

   @Test
  def shutdownOfAllSupervisionedObjects = {
    val latch = new CountDownLatch(1)
    val c1 = newSupervisedConnection("mynode1", ONE_CONN_PER_NODE)
    val wc1 = newSupervisedWriteChannel(c1)
    val rc1 = newSupervisedReadChannel(c1)
    val c2 = newSupervisedConnection("mynode2", ONE_CONN_PER_NODE)
    val wc2 = newSupervisedWriteChannel(c2)
    val rc2 = newSupervisedReadChannel(c2)
    val c3 = newSupervisedConnection("mynode3", ONE_CONN_PER_NODE)
    val wc3 = newSupervisedWriteChannel(c3)
    val rc3 = newSupervisedReadChannel(c3)
    val c4 = newSupervisedConnection("mynode4", ONE_CONN_PER_NODE)
    val wc4 = newSupervisedWriteChannel(c4)
    val rc4 = newSupervisedReadChannel(c4)
    latch.await(1, SECONDS)
    shutdownAll
    latch.await(1, SECONDS)
    linkedCount should be (0)
  }

}