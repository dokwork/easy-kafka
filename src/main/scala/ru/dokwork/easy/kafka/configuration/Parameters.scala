package ru.dokwork.easy.kafka.configuration

trait Parameter[A] {
  val default: A
}

trait Parameters extends Iterable[(Parameter[_], Any)] {
  def get[A](implicit p: Parameter[A]): A
  def +[A](value: A)(implicit p: Parameter[A]): Parameters
  def iterator: Iterator[(Parameter[_], Any)]
}

object Parameters {
  private case class Params(params: Map[Parameter[_], Any]) extends Parameters {
    def get[A](implicit p: Parameter[A]): A = params.getOrElse(p, p.default).asInstanceOf[A]
    def +[A](value: A)(implicit p: Parameter[A]): Parameters = {
      copy(params + (p → value))
    }
    def iterator: Iterator[(Parameter[_], Any)] = params.iterator
  }

  val empty: Parameters = Params(Map())
}
