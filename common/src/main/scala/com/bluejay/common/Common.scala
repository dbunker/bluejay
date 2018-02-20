package com.bluejay.common

import org.json4s.{ DefaultFormats, JInt, JString, JValue }

object Common {

  def extractString(jval: JValue): String = {
    implicit val formats: DefaultFormats = DefaultFormats
    jval match {
      case _: JString => jval.extract[String]
      case _          => ""
    }
  }

  def extractInt(jval: JValue): Int = {
    implicit val formats: DefaultFormats = DefaultFormats
    jval match {
      case _: JInt => jval.extract[Int]
      case _       => 0
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit final class AnyOps[A](self: A) {
    def ===(other: A): Boolean = self == other
  }
}
