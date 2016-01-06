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
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.log4j.Logger

object VecExampleProcess {

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        // Input data: Each row is a bag of words from a sentence or document.
        val documentDF = sqlContext.createDataFrame(Seq(
            "a large company is Gamma".split(" "),
            "a large company is Delta".split(" "),
            "Gamma works with people".split(" "),
            "Delta works near people".split(" "),
            "checked that Gamma sees the future".split(" "),
            "checked that Delta sees some future".split(" "),
            "finds many things Gamma does".split(" "),
            "wants many things Delta does".split(" ")
        ).map(Tuple1.apply)).toDF("text")

        // Learn a mapping from words to Vectors.
        val word2Vec = new Word2Vec()
            .setInputCol("text")
            .setOutputCol("result")
            .setVectorSize(300)
            .setMinCount(0)

        val model : Word2VecModel = word2Vec.fit(documentDF)
        
        val result = model.transform(documentDF)
        result.show()
        
        val synonymsGamma = model.findSynonyms("Gamma", 20)
        synonymsGamma.orderBy("similarity").show()
        val synonymsDelta = model.findSynonyms("Delta", 20)
        synonymsDelta.orderBy("similarity").show()
    }
}