package akka.event

/**
 * Created by IntelliJ IDEA.
 * User: thadeu
 * Date: 8/29/11
 * Time: 10:52 PM
 * To change this template use File | Settings | File Templates.
 */
object EventHandler{

  def error(any: Any, data: String){
    println(any + " " + data)
  }

  def info(any: Any, data: String){
    println(any + " " + data)
  }

  def warning(any: Any, data: String){
    println(any + " " + data)
  }
}