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
import org.apache.spark.sql.functions._

import org.apache.log4j.Logger

object VecExampleProcess {

    def getSequence() :  Seq[Tuple1[Array[String]]] = {
      return Seq(
          "Gamma is a large company".split(" "),
          "Delta is a large company".split(" "),
          "Gamma works with people, Delta does not".split(" "),
          "Delta works with people, Gamma does not".split(" "),
          "checked that Gamma sees the future".split(" "),
          "checked that Delta sees the future".split(" "),
          "finds many things Gamma does".split(" "),
          "wants many things Delta does".split(" "),
          "wants many things Gamma does".split(" "),
          "finds many things Delta does".split(" "),
          "Gamma does".split(" "),
          "Delta does".split(" "),
          "checked Gamma does and Delta does not".split(" "),
          "checked Delta does and Gamma does not".split(" ")
      ).map(Tuple1.apply)
    }

    def generate() {

      val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      // Input data: Each row is a bag of words from a sentence or document.
      val documentDF = sqlContext.createDataFrame(getSequence()).toDF("text")

      // Learn a mapping from words to Vectors.
      val word2Vec = new Word2Vec()
          .setInputCol("text")
          .setOutputCol("result")
          .setVectorSize(300)
          .setMinCount(0)

      val model : Word2VecModel = word2Vec.fit(documentDF)

      val result = model.transform(documentDF)
      result.show()

      // DataFrame
      val synonymsGamma : Dataset[Row] = model.findSynonyms("Gamma", 20)
      synonymsGamma.orderBy(desc("similarity")).show()

      val synonymsGammaRow : Row = synonymsGamma.first()
      val mostSimilarToGamma : String = synonymsGammaRow.getString(0)
      assert(mostSimilarToGamma == "Delta", "Expected Delta, got: " + mostSimilarToGamma)

      val synonymsDelta : Dataset[Row] = model.findSynonyms("Delta", 20)
      synonymsDelta.orderBy(desc("similarity")).show()

      val synonymsDeltaRow : Row = synonymsDelta.first()
      val mostSimilarToDelta : String = synonymsDeltaRow.getString(0)
      assert(mostSimilarToDelta == "Gamma", "Expected Gamma, got: " + mostSimilarToDelta)
    }

    def main(args: Array[String]) {
        generate()
    }
}
