package br.ime.usp.sagat.mock

import java.util.concurrent.ConcurrentHashMap


class RemoteClientMock(val host: String, val port: Int) {
  require(host != null)
  require(port > 0)

  def connect{

  }

  def shutdown{

  }

  def send(message: String){

  }
}

object RemoteClientMock {
  private val remoteClients = new ConcurrentHashMap[String,RemoteClientMock]()

  def clientFor(host: String, port: Int): RemoteClientMock = synchronized{
    val key = host + port
    if(remoteClients.containsKey(key)) remoteClients.get(key)
    else {
      val client = new RemoteClientMock(host, port)
      client.connect
      remoteClients.put(key, client)
      client
    }
  }
}