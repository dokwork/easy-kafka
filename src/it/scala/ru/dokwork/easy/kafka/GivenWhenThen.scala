package ru.dokwork.easy.kafka

import org.scalactic.source.Position
import org.scalatest.Informing

trait GivenWhenThen extends org.scalatest.GivenWhenThen { this: Informing =>

  override def Given(message: String)(implicit pos: Position): Unit = log("Given", message, super.Given)
  override def When(message: String)(implicit pos: Position): Unit = log("When", message, super.When)
  override def Then(message: String)(implicit pos: Position): Unit = log("Then", message, super.Then)

  private def log(task: String, msg: String, f: (String) => Unit): Unit = {
    println("\u001B[33m" + task +": " + msg + "\u001B[0m")
    f(msg)
  }
}
