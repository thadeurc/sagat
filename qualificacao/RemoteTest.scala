case object Ping
case object Pong
case object Quit

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.Exit
import scala.actors.remote.RemoteActor._
import scala.actors.remote.Node

object RemotePingApp {
    def main(args: Array[String]) : Unit = {
        val port = args(0).toInt
        val peer = Node(args(1), args(2).toInt)
        val ping = new RemotePing(port, peer, 16)
        ping.start()
    }
}
class RemotePing(port: Int, peer: Node, count: Int) extends Actor {
    trapExit = true // (1)


	def act() {
		alive(port)     // (2)
		register('Ping, self) // (3)

		val pong = select(peer, 'Pong) // (4)
		link(pong)             // (5)

		var pingsLeft = count - 1
		pong ! Ping     // (6)
		while (true) {
  			receive {     // (7)
    			case Pong =>
				     Console.println("Ping: pong")
				     if (pingsLeft > 0) {
				       pong ! Ping
				       pingsLeft -= 1
				     } else {
				       Console.println("Ping: start termination")
				       pong ! Quit     // (8)
				       // Terminate ping after Pong exited (by linking)
				     }
				case Exit(pong, 'normal) => // (9)
				       Console.println("Ping: stop")
				       exit()
			}
		}

	}
}