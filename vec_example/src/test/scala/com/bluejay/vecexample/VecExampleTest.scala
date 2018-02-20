package com.bluejay.vecexample

import com.bluejay.vecexample.VecExampleProcess._
import org.scalatest.Matchers._
import org.scalatest._

class VecExampleTest extends FlatSpec {

  it should "check text sequence" in {

    val seq: Seq[Tuple1[Array[String]]] = wordsSequence()
    val tup: Tuple1[Array[String]]      = seq.head
    val arr: Seq[String]                = tup._1

    arr(0) should equal("Gamma")
    arr(1) should equal("is")
    arr(2) should equal("a")
  }
}
