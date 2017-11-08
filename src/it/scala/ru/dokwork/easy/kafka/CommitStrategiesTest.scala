package ru.dokwork.easy.kafka

import java.util.TimerTask
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.consumer.OffsetResetStrategy

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class CommitStrategiesTest extends ITest {

  var sending: TimerTask = _

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
      sending = Timer.schedule(sendPeriod) {
        await(producer.send(topic, key, s"commitable_message_" + i.incrementAndGet()))
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

  override def consumerBuilder = super.consumerBuilder.withOffsetResetStrategy(OffsetResetStrategy.EARLIEST)

  override def afterAll() = {
    if (sending ne null) sending.cancel()
  }

  private def createPollingWithBuffer(
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
}
