package ru.dokwork.easy.kafka

import org.apache.kafka.clients.producer.{ Callback, Producer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.scalatest.{ FreeSpec, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable }


class KafkaProducerSpec extends FreeSpec
  with Matchers
  with MockitoSugar {

  type K = String
  type V = String

  private def await(w: Awaitable[_]) = Await.result(w, 1.second)

  private val stubMetadata = new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0)

  trait Fixture {
    val javaProducer = mock[Producer[K, V]]
    val producer = new KafkaProducer[K, V](javaProducer)
  }

  "KafkaProducer" - {
    "should return Future with RecordMetadata after successfully sent" in new Fixture {
      // given:
      when(javaProducer.send(any[ProducerRecord[K, V]], any[Callback]))
        .thenAnswer((_: ProducerRecord[K, V], callback: Callback) => {
          callback.onCompletion(stubMetadata, null)
          mock[java.util.concurrent.Future[RecordMetadata]]
        })
      // when:
      val result = await(producer.send("topic", "Hello!"))
      // then:
      result should be(stubMetadata)
    }
    "should return failed Future after exception on send data to the Kafka" in new Fixture {
      // given:
      when(javaProducer.send(any[ProducerRecord[K, V]], any[Callback]))
        .thenAnswer((_: ProducerRecord[K, V], callback: Callback) => {
          callback.onCompletion(null, TestException())
          mock[java.util.concurrent.Future[RecordMetadata]]
        })
      // when:
      intercept[TestException] {
        await(producer.send("topic", "Hello!"))
      }
    }
  }
}
