package ru.dokwork.easy.kafka

import scala.concurrent.Future

/**
 * Stoppable is a mixin trait to describe process which can be stopped.
 */
trait Stoppable {

  /**
   * Stop the process. The returned Future is completed when the process
   * has been stopped.
   */
  def stop(): Future[Unit]
}
