package ru.dokwork.easy.kafka

import scala.concurrent.Awaitable

/**
 * Stoppable is a mixin trait to describe process which can be stopped.
 */
trait Stoppable {

  /**
   * Stop the process.
   */
  def stop(): Awaitable[Unit]
}
