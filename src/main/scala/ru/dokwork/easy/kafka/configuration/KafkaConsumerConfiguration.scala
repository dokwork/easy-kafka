package ru.dokwork.easy.kafka.configuration

import org.apache.kafka.clients.{ consumer => kafka }
import org.apache.kafka.common.serialization.Deserializer
import ru.dokwork.easy.kafka.{ KafkaConsumer, configuration }
import ru.dokwork.easy.kafka.KafkaConsumer._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Configuration for the [[ru.dokwork.easy.kafka.KafkaConsumer]].
 *
 * @tparam K type of the key of the records in kafka.
 * @tparam V type of the value of the records in kafka.
 * @tparam BS compile-time check that bootstrap servers are defined.
 * @tparam KD compile-time check that key deserializer is defined.
 * @tparam VD compile-time check that value deserializer is defined.
 * @tparam GID compile-time check that group id is defined.
 */
final class KafkaConsumerConfiguration[K, V, BS <: IsDefined, KD <: IsDefined, VD <: IsDefined, GID <: IsDefined] private (
  override protected val params: Parameters = Parameters.empty,
  private val keyDeserializer: Deserializer[K] = null,
  private val valueDeserializer: Deserializer[V] = null
) extends KafkaConfiguration[K, V, KafkaConsumerConfiguration[K, V, BS, KD, VD, GID]] {

  private type CurrentConfiguration = KafkaConsumerConfiguration[K, V, BS, KD, VD, GID]

  /**
   * Specifies the list of "host:port" pairs which will be used for establishing the initial connection
   * to the Kafka cluster. Must be defined.
   */
  def withBootstrapServers(bootstrapServers: => Seq[String]) = {
    val p = params.get[configuration.BootstrapServers].copy(bootstrapServers)
    configure(params + p, keyDeserializer, valueDeserializer)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, Defined, KD, VD, GID]]
  }

  /**
   * Adds the group id to the kafka properties. Must be defined.
   */
  def withGroupId(groupId: String) = {
    val p = params.get[GroupId].copy(Some(groupId))
    configure(params + p, keyDeserializer, valueDeserializer)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, BS, KD, VD, Defined]]
  }

  /**
   * Specifies a deserializer for the kafka records keys. Must be defined.
   */
  def withKeyDeserializer(deserializer: Deserializer[K]) = {
    configure(params, deserializer, valueDeserializer)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, BS, Defined, VD, GID]]
  }

  /**
   * Specifies a deserializer for the kafka records values. Must be defined.
   */
  def withValueDeserializer(deserializer: Deserializer[V]) = {
    configure(params, keyDeserializer, deserializer)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, BS, KD, Defined, GID]]
  }

  /**
   * Specifies [[org.apache.kafka.clients.consumer.OffsetResetStrategy offset reset strategy]] for consumer.
   */
  def withOffsetResetStrategy(strategy: kafka.OffsetResetStrategy) = {
    val p = params.get[configuration.OffsetResetStrategy].copy(strategy)
    configure(params + p)
  }

  /**
   * Specifies a default commit strategy. Default is
   * [[KafkaConsumer.AutoCommitStrategy]] for consumer.
   *
   * @see [[KafkaConsumer.CommitStrategy]]
   */
  object withCommitStrategy {

    /**
     * If this strategy selected then will be used auto-commit from the Kafka.
     *
     * @see <a href="https://kafka.apache.org/documentation/#configuration">enable.auto.commit</a>
     */
    def AutoCommit: CurrentConfiguration = setCommitStrategy(AutoCommitStrategy)

    /**
     * If this strategy selected then all records which were polled and successfully handled
     * will be committed before next poll.
     */
    def CommitEveryPoll: CurrentConfiguration = setCommitStrategy(CommitEveryPollStrategy)

    /**
     * If this strategy selected then nothing will be committed.
     */
    def DoNotCommit: CurrentConfiguration = setCommitStrategy(DoNotCommitStrategy)

    private def setCommitStrategy(strategy: KafkaConsumer.CommitStrategy): CurrentConfiguration = {
      val p = params.get[CommitStrategy].copy(strategy)
      configure(params + p, keyDeserializer, valueDeserializer)
    }
  }

  /**
   * Adds [[java.lang.Runtime#addShutdownHook(java.lang.Thread) shutdown hook]] for
   * [[ru.dokwork.easy.kafka.KafkaConsumer.Polling#stop() stop]] every
   * polling when JVM is shutting down.
   *
   * @param closeTimeout max time to wait polling.
   */
  def finalizeEveryPollWithin(closeTimeout: Duration): CurrentConfiguration = {
    val hook = (p: Polling) => new Runnable {
        override def run(): Unit = Await.result(p.stop(), closeTimeout)
    }
    val p = params.get[ShutdownHook].copy(hook = Some(hook))
    configure(params + p, keyDeserializer, valueDeserializer)
  }

  /**
   * Should create new self instance with amended parameters.
   */
  override protected def configure(params: Parameters): CurrentConfiguration = {
    configure[BS, KD, VD, GID](params, keyDeserializer, valueDeserializer)
  }

  private def configure[BS1 <: IsDefined, KD1 <: IsDefined, VD1 <: IsDefined, GID1 <: IsDefined](
    params: Parameters,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): KafkaConsumerConfiguration[K, V, BS1, KD1, VD1, GID1] = {
    new KafkaConsumerConfiguration[K, V, BS1, KD1, VD1, GID1](params, keyDeserializer, valueDeserializer)
  }

  /**
   * Creates new instance of the type [[ru.dokwork.easy.kafka.KafkaConsumer]].
   */
  def build(
    implicit ev: KafkaConsumerConfiguration[K, V, BS, KD, VD, GID] is KafkaConsumerConfiguration[K, V, Defined, Defined, Defined, Defined]
  ): KafkaConsumer[K, V] = {
    require(keyDeserializer ne null)
    require(valueDeserializer ne null)
    val factory = () => {
      new kafka.KafkaConsumer[K, V](
        properties(),
        keyDeserializer,
        valueDeserializer
      )
    }
    new KafkaConsumer[K, V](factory, params.get[CommitStrategy].strategy, params.get[ShutdownHook].hook)
  }
}

object KafkaConsumerConfiguration {
  def apply[K, V]() = new KafkaConsumerConfiguration[K, V, Undefined, Undefined, Undefined, Undefined]()
}