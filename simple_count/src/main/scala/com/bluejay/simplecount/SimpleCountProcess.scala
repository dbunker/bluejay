package com.bluejay.simplecount

import com.bluejay.common.Common
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object SimpleCountProcess {

  def getBody(jsonString: String): String = {
    val parsed = parse(jsonString)
    Common.extractString(parsed \ "body")
  }

  def main(args: Array[String]): Unit = {

    val inPath           = args(0).toString
    val outPath          = args(1).toString
    val numberPartitions = args(2).toInt

    println(inPath)
    println(outPath)
    println(numberPartitions)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc   = new SparkContext(conf)

    // 1 partition
    val jsonData: RDD[String] = sc.textFile(inPath, numberPartitions)

    val comments = jsonData.map { jsonString =>
      {
        try {
          val body = getBody(jsonString)
          body
        } catch {
          case e: Exception => compact(render("error" -> e.getMessage))
        }
      }
    }

    println(outPath)
    comments.saveAsTextFile(outPath)
  }
}
