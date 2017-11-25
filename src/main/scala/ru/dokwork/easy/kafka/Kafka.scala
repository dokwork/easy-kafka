package ru.dokwork.easy.kafka

import ru.dokwork.easy.kafka.configuration.{
  KafkaConsumerConfiguration,
  KafkaProducerConfiguration
}

/**
  * Builders for the kafka clients such as consumer and producer.
  */
object Kafka {

  object producer {

    /**
      * Creates new thread-safe builder for the [[KafkaProducer KafkaProducer]].
      *
      * @tparam K type of the key of the records in kafka.
      * @tparam V type of the value of the records in kafka.
      */
    def apply[K, V] = KafkaProducerConfiguration[K, V]()
  }

  object consumer {

    /**
      * Creates new thread-safe builder for the [[KafkaConsumer KafkaConsumer]].
      *
      * @tparam K type of the key of the records in kafka.
      * @tparam V type of the value of the records in kafka.
      */
    def apply[K, V] = KafkaConsumerConfiguration[K, V]()
  }

}
