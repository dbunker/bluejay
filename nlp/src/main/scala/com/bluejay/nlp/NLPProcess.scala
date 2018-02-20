package com.bluejay.nlp

import java.io.StringReader

import edu.stanford.nlp.ie.{ AbstractSequenceClassifier, ClassifierCombiner }
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.process.{ Morphology, PTBTokenizer }
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._

import com.bluejay.common.Common._

object NLPProcess {

  val taggerModel = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"

  val tagger: MaxentTagger = new MaxentTagger(taggerModel)

  val classifierA: AbstractSequenceClassifier[CoreLabel] =
    CRFClassifier.getClassifier("edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz")

  val classifierB: AbstractSequenceClassifier[CoreLabel] =
    CRFClassifier.getClassifier("edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz")

  val classifierC: AbstractSequenceClassifier[CoreLabel] =
    CRFClassifier.getClassifier("edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz")

  val classifier: AbstractSequenceClassifier[CoreLabel] = new ClassifierCombiner(classifierA, classifierB, classifierC)

  // for removing html and extended char text
  private val toRemove = "((<[^>]*>)|([^\\x20-\\x7E]))".r

  def tagTokens(text: String): (String, String, String, String) = {

    val strippedText = toRemove.replaceAllIn(text, " ")

    val coreLabels =
      PTBTokenizer.newPTBTokenizer(new StringReader(strippedText), false, false).tokenize()

    tagger.tagCoreLabels(coreLabels)

    val coreLabelsScala = coreLabels.asScala

    val morpha = new Morphology()

    for (coreLabel <- coreLabelsScala) {
      morpha.stem(coreLabel)
    }

    val _ = classifier.classify(coreLabels)

    coreLabelsScala.foldLeft(("", "", "", "")) {
      case ((tokenAcc, lemmaAcc, tagAcc, entAcc), token) => {
        val space = if (tokenAcc === "") "" else " "
        (tokenAcc + space + token.get(classOf[TextAnnotation]),
         lemmaAcc + space + token.get(classOf[LemmaAnnotation]),
         tagAcc + space + token.get(classOf[PartOfSpeechAnnotation]),
         entAcc + space + token.get(classOf[AnswerAnnotation]))
      }
    }
  }

  // collect and process reddit comment bodies
  def main(args: Array[String]): Unit = {

    val inPath           = args(0).toString
    val outPath          = args(1).toString
    val numberPartitions = args(2).toInt

    println(inPath)
    println(outPath)
    println(numberPartitions)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc   = new SparkContext(conf)

    // 32 partitions
    val jsonData: RDD[String] = sc.textFile(inPath, numberPartitions)

    val comments = jsonData.map { jsonString =>
      {
        try {
          val parsed = parse(jsonString)
          val body   = extractString(parsed \ "body")

          val (tokenData, lemmaData, tagData, entData) = tagTokens(body)

          compact(
            render(parsed merge (("tokens" -> tokenData) ~
            ("lemmas"                      -> lemmaData) ~ ("tags" -> tagData) ~ ("ents" -> entData))))

        } catch {
          case e: Exception => compact(render("error" -> e.getMessage))
        }
      }
    }

    println(outPath)
    comments.saveAsTextFile(outPath)
  }
}
