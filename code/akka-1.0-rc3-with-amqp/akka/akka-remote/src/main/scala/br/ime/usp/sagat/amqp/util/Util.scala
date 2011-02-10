package br.ime.usp.sagat.amqp.util


trait Logging {
 @transient val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)
}

