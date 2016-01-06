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

import org.joda.time.DateTime
import java.text.ParseException

import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.dcoref.CorefChain
import edu.stanford.nlp.dcoref.CorefCoreAnnotations._
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import edu.stanford.nlp.semgraph.SemanticGraph
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import edu.stanford.nlp.process.Morphology
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.process.PTBTokenizer
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ie.ClassifierCombiner

import java.util.Properties
import java.util.List
import java.util.Map
import collection.JavaConversions._

import java.io.StringReader

object NLPProcess {
    
    val taggerModel = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
    val tagger : MaxentTagger = new MaxentTagger(taggerModel)
    
    val classifierA : AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier("edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz")
    val classifierB : AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier("edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz")
    val classifierC : AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier("edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz")
    val classifier : AbstractSequenceClassifier[CoreLabel] = new ClassifierCombiner(classifierA, classifierB, classifierC)
    
    // for removing html and extended char text
    val toRemove = "((<[^>]*>)|([^\\x20-\\x7E]))".r
    
    def tagTokens(text : String) : (String, String, String, String) = {

        val strippedText = toRemove.replaceAllIn(text, " ")
        val coreLabels : List[CoreLabel] = PTBTokenizer.newPTBTokenizer(new StringReader(strippedText), false, false).tokenize()
        tagger.tagCoreLabels(coreLabels)
        val morpha = new Morphology()
        for (coreLabel <- coreLabels) { morpha.stem(coreLabel) }
        classifier.classify(coreLabels)
        
        coreLabels.foldLeft(("","","","")) {
            case ((tokenAcc, lemmaAcc, tagAcc, entAcc), token) => {
                val space = if (tokenAcc == "") "" else " "
                (tokenAcc + space + token.get(classOf[TextAnnotation]), 
                lemmaAcc + space + token.get(classOf[LemmaAnnotation]),
                tagAcc + space + token.get(classOf[PartOfSpeechAnnotation]), 
                entAcc + space + token.get(classOf[AnswerAnnotation]))
            }
        }
    }
    
    def extractString(jval: JValue) : String = {
        implicit val formats = DefaultFormats 
        if (jval.isInstanceOf[JString]) jval.extract[String] else ""
    }

    // collect and process reddit comment bodies
    def main(args: Array[String]) {

        val inPath = args(0).toString
        val outPath = args(1).toString
        println(inPath)
        println(outPath)
        
        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        
        // 32 partitions
        val jsonData : RDD[String] = sc.textFile(inPath, 32)

        val comments = jsonData.map { jsonString => {
            try {
                val parsed = parse(jsonString)
                val body = extractString(parsed \ "body")
            
                val (tokenData, lemmaData, tagData, entData) = tagTokens(body)

                compact(render(parsed merge (("tokens" -> tokenData) ~ 
                    ("lemmas" -> lemmaData) ~ ("tags" -> tagData) ~ ("ents" -> entData))))
            
            } catch {
                case e: Exception => compact(render(("error" -> e.getMessage())))
            }
        }}
        
        println(outPath)
        comments.saveAsTextFile(outPath)
    }
}