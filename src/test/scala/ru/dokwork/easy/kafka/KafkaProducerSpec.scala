package ru.dokwork.easy.kafka

import org.apache.kafka.clients.producer.{ Callback, Producer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.scalatest.FreeSpec

import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable }


class KafkaProducerSpec extends FreeSpec with MockitoSugar {

  type K = String
  type V = String

  private def await(w: Awaitable[_]) = Await.result(w, 1.second)

  private val stubMetadata = new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0)

  trait Fixture {
    val javaProducer = mock[Producer[K, V]]
    when(javaProducer.send(any[ProducerRecord[K, V]], any[Callback]))
      .thenAnswer((_: ProducerRecord[K, V], callback: Callback) => {
        callback.onCompletion(stubMetadata, null)
        mock[java.util.concurrent.Future[RecordMetadata]]
      })
    val producer = new KafkaProducer[K, V](javaProducer)
  }

  "KafkaProducer" - {
    "should invoke method send(ProducerRecord<K,V>, Callback)" in new Fixture {
      // when:
      await(producer.send("topic", "Hello!"))
      // then:
      verify(javaProducer).send(any[ProducerRecord[K, V]], any[Callback])
    }
  }
}
