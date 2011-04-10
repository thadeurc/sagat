/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote

import akka.util.Duration
import akka.config.Config._
import akka.config.ConfigurationException

object RemoteClientSettings {
  val SECURE_COOKIE: Option[String] = config.getString("akka.remote.secure-cookie", "") match {
    case "" => None
    case cookie => Some(cookie)
  }
  val RECONNECTION_TIME_WINDOW = Duration(config.getInt("akka.remote.client.reconnection-time-window", 600), TIME_UNIT).toMillis
  val READ_TIMEOUT       = Duration(config.getInt("akka.remote.client.read-timeout", 1), TIME_UNIT)
  val RECONNECT_DELAY    = Duration(config.getInt("akka.remote.client.reconnect-delay", 5), TIME_UNIT)
  val MESSAGE_FRAME_SIZE = config.getInt("akka.remote.client.message-frame-size", 1048576)

  object AMQP{
    lazy val CLIENT_CONNECTION_POLICY   = config.getString("akka.remote.amqp.policy.connection.client","ONE_CONN_PER_NODE")
    lazy val CLIENT_ID_SUFFIX           = config.getString("akka.remote.amqp.policy.storage.client_id.suffix", System.currentTimeMillis.toHexString)
  }
}


object AMQPSettings{
  lazy val BROKER_HOST                = config.getString("akka.remote.amqp.broker.host","localhost")
  lazy val BROKER_VIRTUAL_HOST        = config.getString("akka.remote.amqp.broker.virtualhost","/actor_host")
  lazy val BROKER_USERNAME            = config.getString("akka.remote.amqp.broker.username","actor_admin")
  lazy val BROKER_PASSWORD            = config.getString("akka.remote.amqp.broker.password","actor_admin")
  lazy val STORAGE_AND_CONSUME_POLICY = config.getString("akka.remote.amqp.policy.storage.mode","EXCLUSIVE_TRANSIENT_AUTODELETE")
}

object RemoteServerSettings {
  val isRemotingEnabled = config.getList("akka.enabled-modules").exists(_ == "remote")
  val MESSAGE_FRAME_SIZE = config.getInt("akka.remote.server.message-frame-size", 1048576)
  val SECURE_COOKIE      = config.getString("akka.remote.secure-cookie")
  val REQUIRE_COOKIE     = {
    val requireCookie = config.getBool("akka.remote.server.require-cookie", false)
    if (isRemotingEnabled && requireCookie && SECURE_COOKIE.isEmpty) throw new ConfigurationException(
      "Configuration option 'akka.remote.server.require-cookie' is turned on but no secure cookie is defined in 'akka.remote.secure-cookie'.")
    requireCookie
  }

  val UNTRUSTED_MODE            = config.getBool("akka.remote.server.untrusted-mode", false)
  val HOSTNAME                  = config.getString("akka.remote.server.hostname", "localhost")
  val PORT                      = config.getInt("akka.remote.server.port", 2552)
  val CONNECTION_TIMEOUT_MILLIS = Duration(config.getInt("akka.remote.server.connection-timeout", 1), TIME_UNIT)
  val COMPRESSION_SCHEME        = config.getString("akka.remote.compression-scheme", "zlib")
  val ZLIB_COMPRESSION_LEVEL    = {
    val level = config.getInt("akka.remote.zlib-compression-level", 6)
    if (level < 1 && level > 9) throw new IllegalArgumentException(
      "zlib compression level has to be within 1-9, with 1 being fastest and 9 being the most compressed")
    level
  }

  val SECURE = false

  object AMQP{
    lazy val SERVER_CONNECTION_POLICY   = config.getString("akka.remote.amqp.policy.connection.server","ONE_CONN_PER_NODE")
  }
}
