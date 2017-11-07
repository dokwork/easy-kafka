package ru.dokwork.easy.kafka

import java.util.TimerTask
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class CommitEveryPollTest extends ITest {

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

  override def afterAll() = {
    if (sending ne null) sending.cancel()
  }
}
