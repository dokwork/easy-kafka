package ru.dokwork.easy.kafka.configuration

import org.apache.kafka.clients.{ producer ⇒ kafka }
import org.apache.kafka.common.serialization.Serializer
import ru.dokwork.easy.kafka.KafkaProducer

/**
  * Configuration for the [[ru.dokwork.easy.kafka.KafkaProducer]].
  *
  * @param params
  * @param keySerializer
  * @param valueSerializer
  * @tparam K type of the key of the records in kafka.
  * @tparam V type of the value of the records in kafka.
  * @tparam BS compile-time check that bootstrap servers are defined.
  * @tparam KS compile-time check that key serializer is defined.
  * @tparam VS compile-time check that value serializer is defined.
  */
class KafkaProducerConfiguration[K, V, BS <: IsDefined, KS <: IsDefined, VS <: IsDefined] private (
    override protected val params: Parameters = Parameters.empty,
    private val keySerializer: Serializer[K] = null,
    private val valueSerializer: Serializer[V] = null
) extends KafkaConfiguration[K, V, KafkaProducerConfiguration[K, V, BS, KS, VS]] {

  /**
    * Specifies the list of "host:port" pairs which will be used for establishing the initial connection
    * to the Kafka cluster. Must be defined.
    */
  def withBootstrapServers(bootstrapServers: ⇒ Seq[String]) = {
    val p = params.get[BootstrapServers].copy(bootstrapServers)
    configure(params + p)
      .asInstanceOf[KafkaProducerConfiguration[K, V, Defined, KS, VS]]
  }

  /**
    * Specifies a serializer for the kafka records keys. Must be defined.
    */
  def withKeySerializer(keySerializer: Serializer[K]) = {
    configure(params, keySerializer, valueSerializer)
      .asInstanceOf[KafkaProducerConfiguration[K, V, BS, Defined, VS]]
  }

  /**
    * Specifies a serializer for the kafka records values. Must be defined.
    */
  def withValueSerializer(valueSerializer: Serializer[V]) = {
    configure(params, keySerializer, valueSerializer)
      .asInstanceOf[KafkaProducerConfiguration[K, V, BS, KS, Defined]]
  }

  /**
    * Adds the client id to the kafka properties.
    */
  def withClientId(clientId: String) = {
    val p = params.get[ClientId].copy(Some(clientId))
    configure(params + p)
  }

  /**
    * Creates new instance of the [[ru.dokwork.easy.kafka.KafkaProducer KafkaProducer]].
    */
  def build(
      implicit ev: KafkaProducerConfiguration[K, V, BS, KS, VS] is KafkaProducerConfiguration[
        K,
        V,
        Defined,
        Defined,
        Defined
      ]
  ): KafkaProducer[K, V] = {
    val producer = new kafka.KafkaProducer[K, V](
      properties(),
      keySerializer,
      valueSerializer
    )
    new KafkaProducer[K, V](producer)
  }

  override protected def configure(params: Parameters) = {
    configure[BS, KS, VS](params, keySerializer, valueSerializer)
  }

  protected def configure[BS1 <: IsDefined, KS1 <: IsDefined, VS1 <: IsDefined](
      params: Parameters,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): KafkaProducerConfiguration[K, V, BS1, KS1, VS1] = {
    new KafkaProducerConfiguration[K, V, BS1, KS1, VS1](params, keySerializer, valueSerializer)
  }
}

object KafkaProducerConfiguration {
  def apply[K, V](): KafkaProducerConfiguration[K, V, Undefined, Undefined, Undefined] = {
    new KafkaProducerConfiguration[K, V, Undefined, Undefined, Undefined]()
  }
}
