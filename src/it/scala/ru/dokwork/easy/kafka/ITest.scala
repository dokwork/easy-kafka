package ru.dokwork.easy.kafka

import java.util.TimerTask

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{ Minutes, Span }
import org.scalatest.{ BeforeAndAfterAll, FeatureSpec, Matchers }

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable, Future }

trait ITest extends FeatureSpec
  with BeforeAndAfterAll
  with GivenWhenThen
  with Matchers
  with TimeLimitedTests {

  override def timeLimit = Span(10, Minutes)

  val conf = ConfigFactory.defaultApplication()
  val bootstrapServer = s"${conf.getString("kafka.host")}:${conf.getInt("kafka.port")}"
  info("BOOTSTRAP: " + bootstrapServer)

  def producerBuilder = Kafka.producer[String, String]
    .withBootstrapServers(Seq(bootstrapServer))
    .withKeySerializer(new StringSerializer())
    .withValueSerializer(new StringSerializer())

  def consumerBuilder = Kafka.consumer[String, String]
    .withBootstrapServers(Seq(bootstrapServer))
    .withGroupId("test")
    .withKeyDeserializer(new StringDeserializer())
    .withValueDeserializer(new StringDeserializer())
    .withOffsetResetStrategy(OffsetResetStrategy.EARLIEST)
    .finalizeEveryPollWithin(30.seconds)

  def await(w: Awaitable[_]) = Await.result(w, 1.minute)

  def createPollingWithBuffer(
    consumer: KafkaConsumer[String, String],
    topic: String,
    pollTimeout: Duration,
    key: String
  ) = {
    val receivedMessages = mutable.Buffer[String]()

    lazy val polling = consumer.poll(Seq(topic), pollTimeout) { record =>
      Future.successful {
        if (record.key() == key) receivedMessages.append(record.value())
      }
    }
    (polling, receivedMessages)
  }

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
