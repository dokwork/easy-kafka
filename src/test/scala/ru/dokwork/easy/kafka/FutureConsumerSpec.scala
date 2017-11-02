package ru.dokwork.easy.kafka

import java.util.Collections

import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecords }
import org.mockito.Mockito._
import org.scalatest.FreeSpec

import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable, Promise }

class FutureConsumerSpec extends FreeSpec with MockitoSugar {

  import ru.dokwork.easy.kafka.KafkaConsumer.executor

  private def await(w: Awaitable[_]) = Await.result(w, 1.second)
  private def await(d: Duration) = Thread.sleep(d.toMillis)

  type K = String
  type V = String

  trait Fixture {
    val consumer = mock[Consumer[K, V]]
    val futureConsumer = new FutureConsumer[K, V](consumer)

    def  atSameTimeWithCommit(f: => Unit) = {
      val promise = Promise[Unit]()
      when(consumer.commitSync()).thenAnswer(() => Await.result(promise.future, 30.seconds))
      try {
        f
      } finally {
        promise.success({})
      }
    }

    def atSameTimeWithPoll(f: => Unit) = {
      val promise = Promise[ConsumerRecords[K, V]]()
      when(consumer.poll(any[Long])).thenAnswer((_: Long) => Await.result(promise.future, 30.seconds))
      try {
        f
      } finally {
        promise.success(new ConsumerRecords[K, V](Collections.emptyMap()))
      }
    }
  }

  "FutureConsumer" - {
    "should wakeup consumer for break poll" in new Fixture {
      atSameTimeWithPoll {
        // given:
        val randomPollTimeout = 100L
        // when:
        futureConsumer.poll(randomPollTimeout)
        await(100.milliseconds)
        await(futureConsumer.wakeup())
        // then:
        verify(consumer).wakeup()
      }
    }
    "should not wakeup consumer on commit" in new Fixture {
      atSameTimeWithCommit {
        // when:
        futureConsumer.commit()
        await(100.milliseconds)
        futureConsumer.wakeup()
        // then;
        verify(consumer, never()).wakeup()
      }
    }
  }
}
