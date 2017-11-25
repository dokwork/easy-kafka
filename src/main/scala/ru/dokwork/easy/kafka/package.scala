package ru.dokwork.easy

import scala.concurrent.Future

package object kafka {

  object F {
    val unit: Future[Unit] = Future.successful({})
  }
}
