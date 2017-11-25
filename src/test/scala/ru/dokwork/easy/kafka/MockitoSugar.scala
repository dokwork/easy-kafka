package ru.dokwork.easy.kafka

import org.hamcrest.{ BaseMatcher, Description }
import org.mockito.Matchers.argThat
import org.mockito.Mockito.doAnswer
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.{ Answer, OngoingStubbing }
import org.mockito.verification.VerificationWithTimeout
import org.mockito.{ ArgumentCaptor, Mockito }

import scala.concurrent.duration.Duration
import scala.reflect.Manifest

trait MockitoSugar extends org.scalatest.mockito.MockitoSugar {
  def any[T <: Any](implicit manifest: Manifest[T]): T = {
    org.mockito.Matchers.any(manifest.runtimeClass.asInstanceOf[Class[T]])
  }

  def argumentCaptor[T <: Any](implicit manifest: Manifest[T]): ArgumentCaptor[T] = {
    ArgumentCaptor.forClass(manifest.runtimeClass.asInstanceOf[Class[T]])
  }

  def timeout(duration: Duration): VerificationWithTimeout = {
    Mockito.timeout(duration.toMillis.toInt)
  }

  implicit def Answer0[T](f: () => T) = new Answer[T] {
    override def answer(invocation: InvocationOnMock) = f.apply()
  }

  implicit def Answer1[A, T](f: (A) => T) = new Answer[T] {
    override def answer(i: InvocationOnMock) = f.apply(i.getArguments()(0).asInstanceOf[A])
  }

  implicit def Answer2[A, B, T](f: (A, B) => T) = new Answer[T] {
    override def answer(i: InvocationOnMock) = f.apply(
      i.getArguments()(0).asInstanceOf[A],
      i.getArguments()(1).asInstanceOf[B]
    )
  }

  /**
    * Use `doLazyReturn()` when you want to stub a method that should return lazy
    * computed result.
    *
    * Example:
    *
    * {{{
    *   def f: T = { ... }
    *   doLazyReturn(f).when(mock).someMethod();
    * }}}
    *
    * @param f function which will invoked to compute result of stub's method invocation
    *
    * @return stubber - to select a method for stubbing
    */
  def doLazyReturn[T](f: => T) = doAnswer(() => f)

  /**
    * Add matcher for argument with lazy comparision.
    * Use it to mix [[MockitoSugar#any(scala.reflect.Manifest)]] with
    * real expected values of arguments.
    *
    * Example:
    *
    * {{{
    *   lazy val expectedArg = ...
    *   doReturn(...).when(stub).someMethodWithArgs(arg(expectedArg), any())
    * }}}
    * @param f function which will invoked to get value for comparision with method invocation argument.
    * @tparam T
    * @return
    */
  def arg[T](f: => T) = argThat(
    new BaseMatcher[T] {
      override def matches(item: scala.Any) = f == item

      override def describeTo(description: Description) = ""
    }
  )
}

object MockitoSugar {
  implicit class RichOngoingStubbing[T](val s: OngoingStubbing[T]) extends AnyVal {
    def thenLazyReturn(f: => T): OngoingStubbing[T] = s.thenAnswer(new Answer[T] {
      override def answer(invocation: InvocationOnMock) = f
    })
  }
}
