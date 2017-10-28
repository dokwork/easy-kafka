package ru.dokwork.easy.kafka

import org.apache.kafka.clients.producer.{ Callback, Producer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.FreeSpec

import scala.concurrent.{ Await, Awaitable }
import scala.concurrent.duration._


class KafkaProducerSpec extends FreeSpec {

  private def await(w: Awaitable[_]) = Await.result(w, 1.second)

  "KafkaProducer" - {
    "should invoke method o.a.k.clients.producer.Producer.send(ProducerRecord<K,V>, Callback)" in {
      // given:
      val javaProducer = mockAsyncKafkaProducer[String, String]()
      val producer = new KafkaProducer[String, String](javaProducer)

      // when:
      await(producer.send("topic", "Hello!"))

      // then:
      verify(javaProducer)
        .send(any(classOf[ProducerRecord[String, String]]), any(classOf[Callback]))
    }
  }

  private def mockAsyncKafkaProducer[K, V](): Producer[K, V] = {
    val metadata = new RecordMetadata(new TopicPartition("", 0), 0, 0)
    val producer = mock(classOf[Producer[K, V]])
    doAnswer(
      (invocation: InvocationOnMock) => {
        val callback = invocation.getArguments.apply(1).asInstanceOf[Callback]
        callback.onCompletion(metadata, null)
        mock(classOf[java.util.concurrent.Future[RecordMetadata]])
      }
    ).when(producer).send(any(classOf[ProducerRecord[K, V]]), any(classOf[Callback]))
    producer
  }
}
