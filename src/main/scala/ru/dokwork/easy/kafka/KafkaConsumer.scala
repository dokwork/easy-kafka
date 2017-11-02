package ru.dokwork.easy.kafka

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord, OffsetAndMetadata }
import org.apache.kafka.common.TopicPartition
import ru.dokwork.easy.kafka.KafkaConsumer._

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

/**
 * This implementation allows you to receive messages from kafka and handle every message in one thread.
 *
 * @param consumerFactory factory of the java kafka consumer instance which will used
 *                        for poll kafka.
 * @tparam K type of the key.
 * @tparam V type of the value.
 */
class KafkaConsumer[K, V] private[kafka](
  consumerFactory: () => Consumer[K, V],
  commitStrategy: CommitStrategy
) {

  /**
   * Start poll kafka from specified topics in the new thread and invoke handler for each records
   * which will be received from kafka after poll. Empty iterator will be skipped.
   *
   * @param topics
   * @param pollingTimeout The time, in milliseconds, spent waiting in poll if data is not available.
   *                       If 0, returns immediately with any records that are available now.
   *                       Must not be negative. Default is 100 ms.
   * @param handler        function for handle records which polled from kafka.
   *
   * @return [[KafkaConsumer.Polling]] which indicate polling process. Invoke method close of this future will
   *         stop polling. Never invoke close inside handler!.
   */
  def poll(topics: Seq[String], pollingTimeout: Duration = 100 milliseconds)
    (handler: RecordHandler[K, V]): KafkaConsumer.Polling = {
    val consumer = consumerFactory.apply()
    consumer.subscribe(topics.asJava)
    new PollingImpl(new FutureConsumer(consumer), topics, pollingTimeout.toMillis, handler)
  }

  private class PollingImpl(
    consumer: FutureConsumer[K, V],
    topics: Seq[String],
    pollingTimeout: Long,
    handler: RecordHandler[K, V]
  ) extends KafkaConsumer.Polling {

    import ru.dokwork.easy.kafka.KafkaConsumer.executor

    private val log = Logger(s"$getClass: [${topics.mkString(", ")}]")
    private val isStarted = new AtomicBoolean(true)

    // initialization of this value begins a polling
    private val polling: Future[Unit] = pollKafka().transform { f =>
      consumer.close()
      f
    }

    private def pollKafka(): Future[Unit] = {
      if (isStarted.get) {
        pollOnce()
          .flatMap(itr => whileDo(itr.hasNext && isStarted.get)(handleWithLogging(itr.next)))
          .flatMap(commitIfNeed)
          .flatMap(_ => pollKafka())
      } else {
        Future.unit
      }
    }

    private def whileDo[A](condition: => Boolean)(f: => Future[A]): Future[Seq[A]] = {
      def loop(acc: Seq[A]): Future[Seq[A]] = {
        if (condition) f.flatMap(a => loop(acc :+ a))
        else Future.successful(acc)
      }

      loop(Seq())
    }

    private def pollOnce(): Future[Iterator[ConsumerRecord[K, V]]] = {
      consumer.poll(pollingTimeout).map { recs =>
        log.debug(s"polled ${recs.count()} records")
        recs.iterator().asScala
      }
    }

    private def handleWithLogging(record: ConsumerRecord[K, V]): Future[ConsumerRecord[K, V]] = {
      log.trace(s"begin handle $record")
      val result = handler.apply(record).map(_ => record)
      if (log.underlying.isTraceEnabled) {
        result.onComplete {
          case Success(_) =>
            log.trace(s"handle $record completed successful")
          case Failure(e) =>
            log.trace(s"handle $record failed: $e")
        }
      }
      result
    }

    private def commitIfNeed(records: Seq[ConsumerRecord[K, V]]): Future[Unit] = commitStrategy match {
      case CommitEveryPollStrategy if records.nonEmpty =>
        val beginTime = Deadline.now
        log.debug(s"Begin commit records ${records.mkString("; ")} at $beginTime")
        val commit = consumer.commit()
        commit.onComplete(_ =>
          log.debug(s"Records successful committed in ${Deadline.now - beginTime}")
        )
        commit
      case _ =>
        Future.unit
    }

    override def ready(atMost: Duration)(implicit permit: CanAwait): PollingImpl.this.type = {
      polling.ready(atMost)
      this
    }

    override def result(atMost: Duration)(implicit permit: CanAwait): Unit = {
      polling.result(atMost)
    }

    /**
     * Stop fetch data from kafka and close consumer.
     *
     * @return future that will be completed after last polling completed.
     */
    override def stop(): Future[Unit] = {
      log.info(s"Stop poll topics [${topics.mkString(", ")}]")
      isStarted.set(false)
      consumer.wakeup()
      polling
    }
  }

}

object KafkaConsumer {

  private[kafka] implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(
    Executors.newCachedThreadPool((r: Runnable) => new Thread(r) {
      val threadNumber = new AtomicInteger(0)
      setName(s"${this.getClass.getSimpleName}-thread-" + threadNumber.incrementAndGet())
      setDaemon(true)
    })
  )

  type RecordHandler[K, V] = (ConsumerRecord[K, V]) => Future[Unit]

  type OffsetMap = util.Map[TopicPartition, OffsetAndMetadata]

  trait Polling extends Awaitable[Unit] with Stoppable

  /**
   * Specifies condition for commit to the kafka.
   */
  sealed trait CommitStrategy

  /**
   * Enable automatic offset committing.
   *
   * @see <a href="https://kafka.apache.org/documentation/#configuration">enable.auto.commit</a>
   */
  case object AutoCommitStrategy extends CommitStrategy

  /**
   * If this strategy selected then all records which were polled and successfully handled
   * will be committed before next poll.
   */
  case object CommitEveryPollStrategy extends CommitStrategy

  /**
   * If this strategy selected then nothing will be committed.
   */
  case object DoNotCommitStrategy extends CommitStrategy

}

