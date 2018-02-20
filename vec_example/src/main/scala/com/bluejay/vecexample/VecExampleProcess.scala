package com.bluejay.vecexample

import org.apache.spark.ml.feature.{ Word2Vec, Word2VecModel }
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bluejay.common.Common._

object VecExampleProcess {

  def wordsSequence(): Seq[Tuple1[Array[String]]] =
    Seq(
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

  private def generate() {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(wordsSequence()).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(300)
      .setMinCount(0)

    val model: Word2VecModel = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.show()

    // DataFrame
    val synonymsGamma: Dataset[Row] = model.findSynonyms("Gamma", 20)
    synonymsGamma.orderBy(desc("similarity")).show()

    val synonymsGammaRow: Row      = synonymsGamma.first()
    val mostSimilarToGamma: String = synonymsGammaRow.getString(0)
    assert(mostSimilarToGamma === "Delta", "Expected Delta, got: " + mostSimilarToGamma)

    val synonymsDelta: Dataset[Row] = model.findSynonyms("Delta", 20)
    synonymsDelta.orderBy(desc("similarity")).show()

    val synonymsDeltaRow: Row      = synonymsDelta.first()
    val mostSimilarToDelta: String = synonymsDeltaRow.getString(0)
    assert(mostSimilarToDelta === "Gamma", "Expected Gamma, got: " + mostSimilarToDelta)
  }

  def main(args: Array[String]): Unit =
    generate()
}
