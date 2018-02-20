package com.bluejay.common

import org.scalatest._
import com.bluejay.common.Common._
import Matchers._
import org.json4s.jackson.JsonMethods.parse

class CommonTest extends FlatSpec {

  it should "convert json to only text" in {

    val jsonString = "\"new text\""
    val parsed     = parse(jsonString)

    val body = extractString(parsed)
    body should equal("new text")
  }
}
