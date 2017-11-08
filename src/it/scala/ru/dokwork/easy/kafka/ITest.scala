package ru.dokwork.easy.kafka

import java.util.TimerTask

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{ Minutes, Span }
import org.scalatest.{ BeforeAndAfterAll, FeatureSpec, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable }

trait ITest extends FeatureSpec
  with BeforeAndAfterAll
  with GivenWhenThen
  with Matchers
  with TimeLimitedTests {

  override def timeLimit = Span(5, Minutes)

  val conf = ConfigFactory.defaultApplication()
  val bootstrapServer = s"${conf.getString("kafka.host")}:${conf.getInt("kafka.port")}"
  info("BOOTSTRAP: " + bootstrapServer)

  def groupId: String

  def producerBuilder = Kafka.producer[String, String]
    .withBootstrapServers(Seq(bootstrapServer))
    .withKeySerializer(new StringSerializer())
    .withValueSerializer(new StringSerializer())

  def consumerBuilder = Kafka.consumer[String, String]
    .withBootstrapServers(Seq(bootstrapServer))
    .withGroupId(groupId + "@" + hashCode())
    .withKeyDeserializer(new StringDeserializer())
    .withValueDeserializer(new StringDeserializer())
    .finalizeEveryPollWithin(30.seconds)

  def await(w: Awaitable[_]) = Await.result(w, 1.minute)

  object Timer extends java.util.Timer(true) {
    def schedule(period: Duration)(f: => Unit): TimerTask = {
      val task = new TimerTask {
        override def run(): Unit = f
      }
      schedule(task, 0, period.toMillis)
      task
    }
  }

}
