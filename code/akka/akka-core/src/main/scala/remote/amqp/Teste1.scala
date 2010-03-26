package main.scala.remote.amqp

import se.scalablesolutions.akka.remote.amqp.{RemoteServer, RemoteNode}

/**
 * Created by IntelliJ IDEA.
 * User: thadeurc
 * Date: Mar 26, 2010
 * Time: 1:56:45 AM
 * To change this template use File | Settings | File Templates.
 */

object Teste1 extends Application {
  new RemoteServer().start("localhost",21000)
  new RemoteServer().start("localhost",22000)  

    

}