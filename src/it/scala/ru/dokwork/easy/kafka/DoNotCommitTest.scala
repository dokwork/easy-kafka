package ru.dokwork.easy.kafka

import java.util.TimerTask
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

class DoNotCommitTest extends ITest {

  var sending: TimerTask = _

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

  override def afterAll() = {
    if (sending ne null) sending.cancel()
  }
}
