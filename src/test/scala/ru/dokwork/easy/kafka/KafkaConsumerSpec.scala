package ru.dokwork.easy.kafka

import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord, ConsumerRecords }
import org.mockito.Mockito._
import org.scalatest.FreeSpec

import scala.collection.JavaConverters._
import scala.concurrent.{ Await, Awaitable, Future }
import scala.concurrent.duration._
import scala.util.Random

class KafkaConsumerSpec extends FreeSpec
  with MockitoSugar {

  type K = String
  type V = String

  private def randomConsumerRecord = new ConsumerRecord[K, V]("topic", 0, 0, randomString, randomString)

  private def randomString = Random.alphanumeric.take(10).mkString

  private def await(w: Awaitable[_]) = Await.result(w, 1.second)

  /**
   * Создает имитацию [[org.apache.kafka.clients.consumer.Consumer]]
   * который на каждый поллинг возвращает итератор с одной записью из
   * [[ConsumerFactory#recordsInKafa()]]
   */
  trait ConsumerFactory {
    def recordsInKafka: Seq[ConsumerRecord[K, V]] = Seq.empty

    val consumer = {
      val consumer = mock[Consumer[K, V]]
      val consumerRecords = mock[ConsumerRecords[K, V]]
      doLazyReturn(recordsInKafka.toIterator.asJava).when(consumerRecords).iterator()
      doReturn(consumerRecords).when(consumer).poll(any[Long])
      consumer
    }
    val consumerFactory = () => consumer
  }

  trait Fixture extends ConsumerFactory {
    val topics = Seq("test-topic")

    def commitStrategy = KafkaConsumer.AutoCommitStrategy

    lazy val kafkaConsumer = new KafkaConsumer[K, V](consumerFactory, commitStrategy)
  }

  trait EmptyMockHandler {
    self: Fixture =>
    val mockHandler = mock[KafkaConsumer.RecordHandler[K, V]]
    doReturn(Future.unit).when(mockHandler).apply(any[ConsumerRecord[K, V]])
  }

  trait Random5RecordsInKafka {
    self: Fixture =>
    override val recordsInKafka = (1 to 5).map(_ => randomConsumerRecord)
  }

  "KafkaConsumer" - {
    "should begin polling kafka when invoke method poll" in new Fixture
      // given:
      with EmptyMockHandler with Random5RecordsInKafka {
      // when:
      val polling = kafkaConsumer.poll(topics)(mockHandler)
      Thread.sleep(100)
      // then:
      await(polling.stop())
      verify(consumer, atLeastOnce).poll(any[Long])
    }

    "should stop polling when invoke method close" in new Fixture
      // given:
      with EmptyMockHandler {
      // when:
      val polling = kafkaConsumer.poll(topics)(mockHandler)
      Thread.sleep(100)
      await(polling.stop())
      await(polling)
      // then:
      verify(mockHandler, atMost(1)).apply(any[ConsumerRecord[K, V]])
      verify(consumer).close()
    }

    "should stop polling after exception in handler" in new Fixture
      // given:
      with Random5RecordsInKafka {
      val handler = (_: ConsumerRecord[K, V]) => Future.failed(TestException())
      // when:
      val polling = kafkaConsumer.poll(topics)(handler)
      intercept[TestException] {
        await(polling)
      }
      // then:
      verify(consumer, times(1)).poll(any[Long])
      verify(consumer).close()
    }

    "should stop polling after exception on poll kafka" in new Fixture
      // given:
      with EmptyMockHandler with Random5RecordsInKafka {
      reset(consumer)
      doThrow(TestException()).when(consumer).poll(any[Long])
      // when:
      val polling = kafkaConsumer.poll(topics)(mockHandler)
      intercept[TestException] {
        await(polling)
      }
      // then:
      verify(consumer, times(1)).poll(any[Long])
      verify(consumer).close()
    }
  }
}
