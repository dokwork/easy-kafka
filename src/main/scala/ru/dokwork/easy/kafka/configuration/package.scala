package ru.dokwork.easy.kafka

import org.apache.kafka.clients.consumer
import ru.dokwork.easy.kafka.KafkaConsumer.{ AutoCommitStrategy, Polling }

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
    def asProperties: Seq[(String, String)]
  }

  implicit def tpEquals[A]: A is A = singleton_is.asInstanceOf[A is A]

  // Магия для более точного текста ошибки компиляции
  @implicitNotFound("KafkaClient is not completely configured: ${A}")
  sealed abstract class is[A, B]

  case class Properties(properties: Map[String, String])

  implicit object Properties extends Parameter[Properties] {
    override lazy val default: Properties = new Properties(Map())
  }

  case class BootstrapServers(servers: Seq[String]) extends AsProperty {
    override def asProperties: Seq[(String, String)] =
      Seq("bootstrap.servers" -> servers.mkString(","))
  }

  implicit object BootstrapServers extends Parameter[BootstrapServers] {
    override lazy val default = BootstrapServers(Seq.empty)
  }

  case class ClientId(clientId: Option[String]) extends AsProperty {
    override def asProperties: Seq[(String, String)] = clientId.map(id => ("client.id", id)).toList
  }

  implicit object ClientId extends Parameter[ClientId] {
    override lazy val default: ClientId = ClientId(None)
  }

  case class GroupId(groupId: Option[String]) extends AsProperty {
    override def asProperties: Seq[(String, String)] = groupId.map(id => ("group.id", id)).toList
  }

  implicit object GroupId extends Parameter[GroupId] {
    override lazy val default: GroupId = GroupId(None)
  }

  case class CommitStrategy(strategy: KafkaConsumer.CommitStrategy) extends AsProperty {
    override def asProperties: Seq[(String, String)] = strategy match {
      case AutoCommitStrategy(interval) =>
        Seq(
          "enable.auto.commit" -> "true",
          "auto.commit.interval.ms" -> interval.toMillis.toString
        )
      case _ =>
        Seq("enable.auto.commit" -> "false")
    }
  }

  implicit object CommitStrategy extends Parameter[CommitStrategy] {
    import scala.concurrent.duration._
    override lazy val default: CommitStrategy = CommitStrategy(AutoCommitStrategy(1.second))
  }

  case class OffsetResetStrategy(offsetResetStrategy: consumer.OffsetResetStrategy)
      extends AsProperty {
    override def asProperties: Seq[(String, String)] =
      Seq("auto.offset.reset" -> offsetResetStrategy.toString.toLowerCase())
  }

  implicit object OffsetResetStrategy extends Parameter[OffsetResetStrategy] {
    override lazy val default: OffsetResetStrategy = OffsetResetStrategy(
      consumer.OffsetResetStrategy.LATEST
    )
  }

  case class ShutdownHook(hook: Option[(Polling) => Runnable])

  implicit object ShutdownHook extends Parameter[ShutdownHook] {
    override lazy val default: ShutdownHook = ShutdownHook(None)
  }

}
