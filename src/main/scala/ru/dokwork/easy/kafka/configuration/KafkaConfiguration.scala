package ru.dokwork.easy.kafka.configuration

import java.util

/**
 * Describe common configuration for kafka producer and consumer.
 *
 * @tparam K type of the key of the records in kafka.
 * @tparam V type of the value of the records in kafka.
 * @tparam C type of configuration which extends this.
 */
trait KafkaConfiguration[K, V, C <: KafkaConfiguration[K, V, C]] {

  /**
   * Current stack of the parameters which will be used to build client to the kafka.
   */
  protected val params: Parameters = Parameters.empty

  /**
   * Should create new self instance with amended parameters.
   */
  protected def configure(params: Parameters): C

  /**
   * Specifies a base properties which will be patched by defined properties from
   * [[KafkaConfiguration#params()]].
   */
  def withProperties(properties: (String, String)*): C = {
    val p = params[Properties].copy(properties.toMap)
    configure(params + p)
  }

  /**
   * Builds the [[java.util.Properties Properties]] for kafka with specified parameters.
   */
  protected def properties(): util.Properties = {
    import scala.collection.JavaConverters._

    def onlyDefinedProperties(p: (Parameter[_], Any)) = p match {
      case (_, p: AsProperty) => p.asProperty.isDefined
      case _ => false
    }

    def convertToProperty(p: (Parameter[_], Any)) = p match {
      case (_, p: AsProperty) => p.asProperty.get
    }

    val props = params[Properties].properties ++ params.view
      .filter(onlyDefinedProperties)
      .map(convertToProperty)

    val kafkaProperties = new util.Properties()
    kafkaProperties.putAll(props.asJava)
    kafkaProperties
  }

}
