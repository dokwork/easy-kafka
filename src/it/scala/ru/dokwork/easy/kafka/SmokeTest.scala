package ru.dokwork.easy.kafka

import org.apache.kafka.clients.producer.RecordMetadata

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SmokeTest extends ITest {
  feature("Send and poll messages") {
    val key = hashCode.toString
    info(s"Kafka key for this run is $key")

    Given("Topic: KafkaClientsIT_Smoke")
    val topic = "KafkaClientsIT_Smoke"

    scenario("Smoke test for KafkaProducer and KafkaConsumer") {
      Given("Messages for sent to the kafka")
      val lastMessage = "last@" + hashCode()
      val messages: Seq[String] = (1 to 5).map(i => s"smoke_message_$i") :+ lastMessage

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
}
