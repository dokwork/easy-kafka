package ru.dokwork.easy.kafka.configuration

import org.apache.kafka.clients.{ consumer => kafka }
import org.apache.kafka.common.serialization.Deserializer
import ru.dokwork.easy.kafka.KafkaConsumer
import ru.dokwork.easy.kafka.KafkaConsumer._

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
  override protected val keyDeserializer: Deserializer[K] = null,
  override protected val valueDeserializer: Deserializer[V] = null
) extends ConsumerConfiguration[K, V, BS, KD, VD, GID, KafkaConsumer[K, V]] {


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
    def AutoCommit = setCommitStrategy(AutoCommitStrategy)

    /**
     * If this strategy selected then all records which were polled and successfully handled
     * will be committed before next poll.
     */
    def CommitEveryPoll = setCommitStrategy(CommitEveryPollStrategy)

    /**
     * If this strategy selected then nothing will be committed.
     */
    def DoNotCommit = setCommitStrategy(DoNotCommitStrategy)

    private def setCommitStrategy(strategy: KafkaConsumer.CommitStrategy) = {
      val p = params[CommitStrategy].copy(strategy)
      configure(params + p)
    }
  }

  override protected def build(
    params: Parameters,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): KafkaConsumer[K, V] = {
    def createConsumer() = {
      new kafka.KafkaConsumer[K, V](
        properties(),
        keyDeserializer,
        valueDeserializer
      )
    }

    new KafkaConsumer[K, V](createConsumer, params[CommitStrategy].strategy)
  }

  override protected def configure[BS1 <: IsDefined, KD1 <: IsDefined, VD1 <: IsDefined, GID1 <: IsDefined](
    params: Parameters,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ) = {
    new KafkaConsumerConfiguration[K, V, BS1, KD1, VD1, GID1](params, keyDeserializer, valueDeserializer)
  }

}

object KafkaConsumerConfiguration {
  def apply[K, V]() = new KafkaConsumerConfiguration[K, V, Undefined, Undefined, Undefined, Undefined]()
}