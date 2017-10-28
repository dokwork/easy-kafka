package ru.dokwork.easy.kafka.configuration

import org.apache.kafka.clients.{ consumer => kafka }
import org.apache.kafka.common.serialization.Deserializer
import ru.dokwork.easy.kafka.configuration

/**
 * Describe common configuration for every implementation of the kafka consumer
 * with compile-time check for key, value deserializers and group id.
 *
 * @tparam K   type of the key of the records in kafka.
 * @tparam V   type of the value of the records in kafka.
 * @tparam BS  compile-time check that bootstrap servers are defined.
 * @tparam KD  compile-time check that key deserializer is defined.
 * @tparam VD  compile-time check that value deserializer is defined.
 * @tparam GID compile-time check that group id is defined.
 * @tparam T   type of the consumer realization.
 */
private[configuration] trait ConsumerConfiguration[K, V, BS <: IsDefined, KD <: IsDefined, VD <: IsDefined, GID <: IsDefined, T]
  extends KafkaConfiguration[K, V, ConsumerConfiguration[K, V, BS, KD, VD, GID, T]] {

  protected val keyDeserializer: Deserializer[K] = null
  protected val valueDeserializer: Deserializer[V] = null

  /**
   * Specifies the list of "host:port" pairs which will be used for establishing the initial connection
   * to the Kafka cluster. Must be defined.
   */
  final def withBootstrapServers(bootstrapServers: => Seq[String]) = {
    val p = params[configuration.BootstrapServers].copy(bootstrapServers)
    configure(params + p)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, Defined, KD, VD, GID]]
  }

  /**
   * Adds the group id to the kafka properties. Must be defined.
   */
  final def withGroupId(groupId: String) = {
    val p = params[GroupId].copy(Some(groupId))
    configure(params + p)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, BS, KD, VD, Defined]]
  }

  /**
   * Specifies a deserializer for the kafka records keys. Must be defined.
   */
  final def withKeyDeserializer(deserializer: Deserializer[K]) = {
    configure(params, deserializer, valueDeserializer)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, BS, Defined, VD, GID]]
  }

  /**
   * Specifies a deserializer for the kafka records values. Must be defined.
   */
  final def withValueDeserializer(deserializer: Deserializer[V]) = {
    configure(params, keyDeserializer, deserializer)
      .asInstanceOf[KafkaConsumerConfiguration[K, V, BS, KD, Defined, GID]]
  }

  /**
   * Specifies [[org.apache.kafka.clients.consumer.OffsetResetStrategy offset reset strategy]] for consumer.
   */
  def withOffsetResetStrategy(strategy: kafka.OffsetResetStrategy) = {
    val p = params[configuration.OffsetResetStrategy].copy(strategy)
    configure(params + p)
  }

  /**
   * Should create new self instance with amended parameters.
   */
  override final protected def configure(params: Parameters) = {
    configure[BS, KD, VD, GID](params, keyDeserializer, valueDeserializer)
  }

  /**
   * Creates new instance of the type [[T]].
   */
  final def build(
    implicit ev: ConsumerConfiguration[K, V, BS, KD, VD, GID, T] is ConsumerConfiguration[K, V, Defined, Defined, Defined, Defined, T]
  ): T = {
    require(keyDeserializer ne null)
    require(valueDeserializer ne null)
    build(params, keyDeserializer, valueDeserializer)
  }

  protected def build(params: Parameters, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): T

  protected def configure[BS1 <: IsDefined, KD1 <: IsDefined, VD1 <: IsDefined, GID1 <: IsDefined](
    params: Parameters,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerConfiguration[K, V, BS1, KD1, VD1, GID1, T]
}
