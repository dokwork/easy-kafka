package ru.dokwork.easy.kafka

import scala.util.control.NoStackTrace

/**
 * When you want to test scenario with exception, but doesn't want to see exception in log,
 * you can throw this exception and config you logger to ignore it.
 *
 * Example for logback:
 * {{{
 * <appender>
 *     <filter class="ru.dokwork.easy.kafka.TestException" />
 *     ...
 * </appender>
 * }}}
 */
case class TestException(msg: String = "") extends RuntimeException(msg) with NoStackTrace
