package ru.dokwork.easy.kafka

import java.util.concurrent.atomic.AtomicInteger
import java.util.{ TimerTask, Timer => _ }

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }
import org.scalatest.{ FeatureSpec, Matchers }
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{ Minutes, Span }

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable, Future }

class KafkaClientsIT extends FeatureSpec
  with GivenWhenThen
  with Matchers
  with TimeLimitedTests {

  override def timeLimit = Span(10, Minutes)

  val conf = ConfigFactory.defaultApplication()
  val bootstrapServer = s"${conf.getString("kafka.host")}:${conf.getInt("kafka.port")}"
  info("BOOTSTRAP: " + bootstrapServer)

  val producerBuilder = Kafka.producer[String, String]
    .withBootstrapServers(Seq(bootstrapServer))
    .withKeySerializer(new StringSerializer())
    .withValueSerializer(new StringSerializer())

  val consumerBuilder = Kafka.consumer[String, String]
    .withBootstrapServers(Seq(bootstrapServer))
    .withGroupId("test")
    .withKeyDeserializer(new StringDeserializer())
    .withValueDeserializer(new StringDeserializer())
    .finalizeEveryPollWithin(30.seconds)

  feature("Send and poll messages") {
    val key = hashCode.toString
    info(s"Kafka key for this run is $key")

    Given("Topic: KafkaClientsIT_Smoke")
    val topic = "KafkaClientsIT_Smoke"

    scenario("Smoke test for KafkaProducer and KafkaConsumer") {
      Given("Messages for sent to the kafka")
      val lastMessage = "last@" + hashCode()
      val messages: Seq[String] = (1 to 5).map(i => s"message_$i") :+ lastMessage

      Given("Kafka producer")
      val producer = producerBuilder.build

      Given("Kafka polling which saves all polled messages to the buffer")
      val consumer = consumerBuilder.build
      val receivedMessages = mutable.Buffer[String]()
      lazy val polling: KafkaConsumer.Polling = consumer.poll(Seq(topic)) { record =>
        if (record.key() == key) {
          receivedMessages.append(record.value())
          if (record.value() == lastMessage) polling.stop()
        }
        Future.unit
      }

      When("All messages will be sent to the Kafka")
      val produce: Seq[Future[RecordMetadata]] = messages.map(producer.send(topic, key, _))
      await(Future.sequence(produce))

      Then("Begin poll messages")
      await(polling)

      Then("All messages which were sent should be received")
      receivedMessages should contain theSameElementsAs messages
    }
  }

  feature("Commit after every poll") {
    val key = hashCode.toString
    info(s"Kafka key for this run is $key")

    Given("Topic: KafkaClientsIT_Commit")
    val topic = "KafkaClientsIT_Commit"
    val sendPeriod = 300.milliseconds
    info(s"send period: $sendPeriod")
    val pollTimeout = 700.milliseconds
    info(s"poll timeout: $pollTimeout")

    scenario("Repeated poll should not receive previously handled messages") {
      Given("Kafka consumer with CommitEveryPoll strategy")
      val consumer = consumerBuilder
        .withCommitStrategy.CommitEveryPoll
        .build

      Given("Kafka producer which produce messages in background every 300ms")
      val producer = producerBuilder.build
      val i = new AtomicInteger(0)
      Timer.schedule(sendPeriod) {
        await(producer.send(topic, key, s"message_" + i.incrementAndGet()))
      }

      Given("First kafka polling which saves all polled messages to the buffer")
      val (firstPolling, firstReceivedMessages) = createPollingWithBuffer(consumer, topic, pollTimeout, key)

      When("Received at least 3 records")
      while (firstReceivedMessages.size < 3) Thread.sleep(pollTimeout.toMillis * 2)

      Then("Complete first polling and begin a new one")
      await(firstPolling.stop())
      info(s"...polled ${firstReceivedMessages.size} messages")
      val (secondPolling, secondReceivedMessages) = createPollingWithBuffer(consumer, topic, pollTimeout, key)

      When("Received at least 3 records again then stop second poling")
      while (secondReceivedMessages.size < 3) Thread.sleep(pollTimeout.toMillis * 2)
      await(secondPolling.stop())

      Then("Messages received at second polling should not contain any messages from the first polling")
      val (a :: b :: unexpectedMessages) = firstReceivedMessages.toList
      secondReceivedMessages should contain noneOf(a, b, unexpectedMessages: _*)
    }
  }

  feature("Do not commit after every poll") {
    val key = hashCode.toString
    info(s"Kafka key for this run is $key")

    Given("Topic: KafkaClientsIT_NotCommit")
    val topic = "KafkaClientsIT_NotCommit"
    val sendPeriod = 300.milliseconds
    info(s"send period: $sendPeriod")
    val pollTimeout = 700.milliseconds
    info(s"poll timeout: $pollTimeout")

    scenario("Repeated poll should receive all previously handled messages") {
      Given("Kafka consumer with DoNotCommit strategy")
      val consumer = consumerBuilder
        .withCommitStrategy.DoNotCommit
        .build

      Given("Kafka producer which produce messages in background every 300ms")
      val producer = producerBuilder.build
      val i = new AtomicInteger(0)
      Timer.schedule(sendPeriod) {
        await(producer.send(topic, key, s"message_" + i.incrementAndGet()))
      }

      Given("First kafka polling which saves all polled messages to the buffer")
      val (firstPolling, firstReceivedMessages) = createPollingWithBuffer(consumer, topic, pollTimeout, key)

      When("Received at least 1 record")
      while (firstReceivedMessages.isEmpty) Thread.sleep(pollTimeout.toMillis * 2)

      Then("Complete first polling and begin a new one")
      await(firstPolling.stop())
      info(s"...polled ${firstReceivedMessages.size} messages")
      val (secondPolling, secondReceivedMessages) = createPollingWithBuffer(consumer, topic, pollTimeout, key)

      When("Received more records than first time and stop poling")
      while (secondReceivedMessages.size < firstReceivedMessages.size) Thread.sleep(pollTimeout.toMillis * 2)
      await(secondPolling.stop())

      Then("Messages received at second polling should contain all messages from the first polling")
      secondReceivedMessages should contain allElementsOf firstReceivedMessages
    }
  }

  private def await(w: Awaitable[_]) = Await.result(w, 1.minute)

  private object Timer extends java.util.Timer(true) {
    def schedule(period: Duration)(f: => Unit): Unit = {
      schedule(new TimerTask {
        override def run(): Unit = f
      }, 0, period.toMillis)
    }
  }

  private def createPollingWithBuffer(
    consumer: KafkaConsumer[String, String],
    topic: String,
    pollTimeout: Duration,
    key: String
  ) = {
    val receivedMessages = mutable.Buffer[String]()

    lazy val polling = consumer.poll(Seq(topic), pollTimeout) { record =>
      Future(if (record.key() == key) receivedMessages.append(record.value()))
    }
    (polling, receivedMessages)
  }
}
