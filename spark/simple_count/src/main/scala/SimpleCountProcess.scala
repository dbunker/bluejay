import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.SparkConf

import org.apache.commons.io.FileUtils
import java.io._
import scala.math._
import scala.collection.immutable.ListMap

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import java.text.ParseException

object SimpleCountProcess {

    def extractString(jval: JValue) : String = {
        implicit val formats = DefaultFormats
        if (jval.isInstanceOf[JString]) jval.extract[String] else ""
    }

    def getBody(jsonString: String) : String = {
        val parsed = parse(jsonString)
        val body = extractString(parsed \ "body")
        body
    }

    def main(args: Array[String]) {

        val inPath = args(0).toString
        val outPath = args(1).toString
        val numberPartitions = args(2).toInt
        println(inPath)
        println(outPath)
        println(numberPartitions)

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)

        // 1 partition
        val jsonData : RDD[String] = sc.textFile(inPath, numberPartitions)

        val comments = jsonData.map { jsonString => {
            try {
                val body = getBody(jsonString)
                body
            } catch {
                case e: Exception => compact(render(("error" -> e.getMessage())))
            }
        }}

        println(outPath)
        comments.saveAsTextFile(outPath)
    }
}
