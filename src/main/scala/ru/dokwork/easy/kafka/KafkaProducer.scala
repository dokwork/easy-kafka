package ru.dokwork.easy.kafka

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{ Callback, Producer, ProducerRecord, RecordMetadata }

import scala.concurrent.duration.Deadline
import scala.concurrent.{ Future, Promise }

/**
  * Proxy class to the [[org.apache.kafka.clients.producer.Producer]] with async interface.
  *
  * @param producer java producer to Kafka.
  * @tparam K type of key.
  * @tparam V typ  of message.
  */
class KafkaProducer[K, V] private[kafka] (producer: Producer[K, V]) {

  private val log = Logger(getClass)

  def send(topic: String, value: V): Future[RecordMetadata] = {
    val record: ProducerRecord[K, V] = new ProducerRecord(topic, value)
    send(record)
  }

  private def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    val beginTime = Deadline.now
    producer.send(
      record,
      new Callback {
        override def onCompletion(m: RecordMetadata, e: Exception): Unit = {
          if (e ne null) {
            log.error(s"Exception on send $record in ${Deadline.now - beginTime}", e)
            promise.failure(e)
          } else {
            log.trace(s"Record $record successfully sent in ${Deadline.now - beginTime}")
            promise.success(m)
          }
        }
      }
    )
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
