package ru.dokwork.easy.kafka

import java.util.Collections

import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecords }
import org.apache.kafka.common.errors.WakeupException

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Proxy class to the [[org.apache.kafka.clients.consumer.Consumer]] with async interface.
  *
  * @param consumer
  * @tparam K
  * @tparam V
  */
private[kafka] class FutureConsumer[K, V](consumer: Consumer[K, V]) {

  // hack to avoid issue: https://github.com/dokwork/easy-kafka/issues/1
  private val commitLock = new AnyRef()

  def poll(timeout: Long)(implicit executor: ExecutionContext): Future[ConsumerRecords[K, V]] =
    Future {
      try {
        consumer.poll(timeout)
      } catch {
        case _: WakeupException ⇒
          new ConsumerRecords[K, V](Collections.emptyMap())
      }
    }

  /**
    * Invoke method [[org.apache.kafka.clients.consumer.Consumer#wakeup()]] to stop poll.
    */
  def wakeup()(implicit executor: ExecutionContext): Future[Unit] = Future {
    commitLock.synchronized {
      consumer.wakeup()
    }
  }

  def commit()(implicit executor: ExecutionContext): Future[Unit] = Future {
    commitLock.synchronized {
      // FIXME commitAsync doesn't work. I don't understand why.
      consumer.commitSync()
    }
  }

  def close(): Unit = consumer.close()
}
