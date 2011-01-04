import sbt._ 
 
class AMQPAPIDraftProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject { 
  lazy val scalatest = "org.scalatest" % "scalatest" % "1.2"
  lazy val rabbit = "com.rabbitmq" % "amqp-client" % "2.2.0"
  lazy val slf4j = "org.slf4j" % "slf4j-api" % "1.6.0"
  lazy val logback      = "ch.qos.logback" % "logback-classic" % "0.9.24"
  lazy val logback_core = "ch.qos.logback" % "logback-core" % "0.9.24"
  lazy val junit = "junit" % "junit" % "4.8.2"
  lazy val scalas_specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6"
  lazy val easymock = "org.easymock" % "easymock" % "3.0"
}
