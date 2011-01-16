package br.ime.usp.sagat.amqp.util

import java.util.concurrent.ConcurrentHashMap
import com.rabbitmq.client.{Channel, ConnectionFactory, Connection}

trait ReadAndWriteChannels {
  val writeChannel: Channel
  val readChannel:  Channel
}

trait ReadAndWriteConnections {
  val readConnection:  Connection
  val writeConnection: Connection
}

object ConnectionSharePolicy extends Enumeration {
  type ConnectionSharePolicy = ConnectionSharePolicyParams

  case class ConnectionSharePolicyParams(val channels: Int, enumId: Int) extends Value {
    override def id = this.enumId
  }
  val ONE_CONN_PER_CHANNEL = ConnectionSharePolicyParams(channels = 1, enumId = 1)

  val ONE_CONN_PER_NODE = ConnectionSharePolicyParams(channels = 2, enumId = 2)
}

import ConnectionSharePolicy._

class EnhancedConnection(val readConnection: Connection, val writeConnection: Connection)
    extends ReadAndWriteConnections with ReadAndWriteChannels with ControlStructures {

  require(readConnection != null && writeConnection != null)

  require(readConnection.isOpen && writeConnection.isOpen)

  lazy val readChannel = readConnection.createChannel

  lazy val writeChannel = writeConnection.createChannel

  def isReadOpen = readConnection.isOpen

  def isWriteOpen = writeConnection.isOpen

  def close = {
    silentClose(readConnection)
    silentClose(writeConnection)
  }
}


abstract class ConnectionFactoryWithLimitedChannels(policy: ConnectionSharePolicy) {
  require(policy != null)
  lazy val factory: ConnectionFactory = {
    val cf = new ConnectionFactory
    cf.setHost("localhost")
    cf.setUsername("actor_admin")
    cf.setPassword("actor_admin")
    cf.setVirtualHost("/actor_host")
    cf.setRequestedChannelMax(policy.channels)
    cf.setRequestedHeartbeat(15) /* to confirm the network is ok - value in seconds */
    cf
  }
  def newConnection: Connection = factory.newConnection
}
class ReadAndWriteConnectionFactory extends ConnectionFactoryWithLimitedChannels(ONE_CONN_PER_NODE)
class ReadOrWriteConnectionFactory extends ConnectionFactoryWithLimitedChannels(ONE_CONN_PER_CHANNEL)

trait ConnectionPoolDefinition {
  private val serverConnections = new ConcurrentHashMap[String, EnhancedConnection]()
  private val clientConnections = new ConcurrentHashMap[String, EnhancedConnection]()
  private[sagat] val readOrWriteConnFactory: ReadOrWriteConnectionFactory
  private[sagat] val readAndWriteConnFactory: ReadAndWriteConnectionFactory


  private[sagat] def ensureConnSharePolicy(conn: EnhancedConnection, policy: ConnectionSharePolicy): EnhancedConnection = {
    require(conn.readConnection.getChannelMax == policy.channels)
    require(conn.writeConnection.getChannelMax == policy.channels)
    conn
  }

  def getConnectionForServerBridge(nodeName: String, policy: ConnectionSharePolicy): EnhancedConnection =  {
    var conn: EnhancedConnection = null
    if(serverConnections.containsKey(nodeName)) {
      conn = serverConnections.get(nodeName)
    }else{
      conn = newConnection(policy)
      serverConnections.put(nodeName, conn)
    }
    ensureConnSharePolicy(conn, policy)
  }

  private[sagat] def newConnection(policy: ConnectionSharePolicy): EnhancedConnection = lock.synchronized{
    policy match {
      case ONE_CONN_PER_NODE => {
        val single = readAndWriteConnFactory.newConnection
        new EnhancedConnection(single, single)
      }
      case ONE_CONN_PER_CHANNEL => {
        new EnhancedConnection(readOrWriteConnFactory.newConnection, readOrWriteConnFactory.newConnection)
      }
    }
  }

  def getConnectionForClientBridge(nodeName: String, connPolicy: ConnectionSharePolicy): EnhancedConnection = {
    var conn: EnhancedConnection = null
    if(clientConnections.containsKey(nodeName)) {
      conn = clientConnections.get(nodeName)
    }else{
      conn = newConnection(connPolicy)
      clientConnections.put(nodeName, conn)
    }
    ensureConnSharePolicy(conn, connPolicy)
  }

  def forceDisconnectAll = {
    clientConnections.values.toArray[EnhancedConnection](Array[EnhancedConnection]()).foreach(enh => enh.close)
    serverConnections.values.toArray[EnhancedConnection](Array[EnhancedConnection]()).foreach(enh => enh.close)
    clientConnections.clear
    serverConnections.clear
  }
}

object ConnectionPool extends ConnectionPoolDefinition {
  lazy val readOrWriteConnFactory  =  new ReadOrWriteConnectionFactory
  lazy val readAndWriteConnFactory =  new ReadAndWriteConnectionFactory
}