package ru.dokwork.easy.kafka

import scala.concurrent.Future

trait Stoppable {

  def stop(): Future[Unit]
}
