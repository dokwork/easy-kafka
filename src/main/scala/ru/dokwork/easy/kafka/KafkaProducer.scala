package ru.dokwork.easy.kafka

import org.apache.kafka.clients.producer.{ Callback, Producer, ProducerRecord, RecordMetadata }

import scala.concurrent.{ Future, Promise }

/**
 * This implementation return future when message was sent and doesn't need callback.
 *
 * @param producer java producer to Kafka.
 * @tparam K type of key.
 * @tparam V typ  of message.
 */
class KafkaProducer[K, V] private[kafka](producer: Producer[K, V]) {

  def send(topic: String, value: V): Future[RecordMetadata] = {
    val record: ProducerRecord[K, V] = new ProducerRecord(topic, value)
    send(record)
  }

  private def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    producer.send(record, new Callback {
      override def onCompletion(m: RecordMetadata, e: Exception): Unit = {
        if (e ne null) {
          promise.failure(e)
        } else {
          promise.success(m)
        }
      }
    })
    promise.future
  }

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    val record: ProducerRecord[K, V] = new ProducerRecord(topic, key, value)
    send(record)
  }

  def send(topic: String, part: Int, key: K, value: V): Future[RecordMetadata] = {
    val record: ProducerRecord[K, V] = new ProducerRecord(topic, part, key, value)
    send(record)
  }
}
