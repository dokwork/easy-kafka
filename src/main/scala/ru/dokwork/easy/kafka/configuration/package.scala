package ru.dokwork.easy.kafka

import org.apache.kafka.clients.consumer
import ru.dokwork.easy.kafka.KafkaConsumer.AutoCommitStrategy

import scala.annotation.implicitNotFound

/**
 * Here defined all [[configuration.Parameters parameters]] for configure kafka client.
 */
package object configuration {

  private[this] final val singleton_is = new is[Any, Any] {}

  private[kafka] sealed trait IsDefined

  private[kafka] trait Defined extends IsDefined

  private[kafka] trait Undefined extends IsDefined

  /**
   * Every configuration parameters which extends this trait will be added to the properties
   * on build some client to the kafka.
   */
  trait AsProperty {
    def asProperty: Option[(String, String)]
  }

  implicit def tpEquals[A]: A is A = singleton_is.asInstanceOf[A is A]

  // Магия для более точного текста ошибки компиляции
  @implicitNotFound("KafkaClient is not completely configured: ${A}")
  sealed abstract class is[A, B]

  implicit object Properties extends Parameter[Properties] {
    override val default: Properties = new Properties(Map())
  }

  case class Properties(properties: Map[String, String])

  case class BootstrapServers(servers: Seq[String]) extends AsProperty {
    override def asProperty = Some("bootstrap.servers", servers.mkString(","))
  }

  implicit object BootstrapServers extends Parameter[BootstrapServers] {
    override val default = BootstrapServers(Seq.empty)
  }

  case class ClientId(clientId: Option[String]) extends AsProperty {
    override def asProperty = clientId.map(id => ("client.id", id))
  }

  implicit object ClientId extends Parameter[ClientId] {
    override val default: ClientId = ClientId(None)
  }

  case class GroupId(groupId: Option[String]) extends AsProperty {
    override def asProperty: Option[(String, String)] = groupId.map(id => ("group.id", id))
  }

  implicit object GroupId extends Parameter[GroupId] {
    override val default: GroupId = GroupId(None)
  }

  case class CommitStrategy(strategy: KafkaConsumer.CommitStrategy) extends AsProperty {
    override def asProperty: Option[(String, String)] =
      Some("enable.auto.commit", (strategy == AutoCommitStrategy).toString)
  }

  implicit object CommitStrategy extends Parameter[CommitStrategy] {
    override val default: CommitStrategy = CommitStrategy(AutoCommitStrategy)
  }

  case class OffsetResetStrategy(offsetResetStrategy: consumer.OffsetResetStrategy) extends AsProperty {
    override def asProperty: Option[(String, String)] =
      Some("auto.offset.reset", offsetResetStrategy.toString.toLowerCase())
  }

  implicit object OffsetResetStrategy extends Parameter[OffsetResetStrategy] {
    override val default: OffsetResetStrategy = OffsetResetStrategy(consumer.OffsetResetStrategy.LATEST)
  }
}
