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

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.log4j.Logger

object Word2VecProcess {
    val LOG = Logger.getLogger(this.getClass.getSimpleName);

    abstract class Config extends Serializable {
        val vectorSize : Int = 300
        val numSynonyms : Int = 10
        val minNumWords : Int
        val minNumSubredditComments : Int
        val minOrgCount : Int
    }

    object ProdConfig extends Config {
        override val minNumWords = 10
        override val minNumSubredditComments = 5000
        override val minOrgCount = 40
    }

    object LocalConfig extends Config {
        override val minNumWords = 0
        override val minNumSubredditComments = 30
        override val minOrgCount = 3
    }

    def extractString(jval: JValue) : String = {
        implicit val formats = DefaultFormats
        if (jval.isInstanceOf[JString]) jval.extract[String] else ""
    }

    def extractInt(jval: JValue) : Int = {
        implicit val formats = DefaultFormats
        if (jval.isInstanceOf[JInt]) jval.extract[Int] else 0
    }

    def save[T](outPath: String, data: RDD[T]) {
        println("Write to: " + outPath)
        try {
            data.saveAsTextFile(outPath)
        } catch {
            // in case cannot write file
            case e: Exception => {
                val msg = s"Could not write to $outPath"
                println(msg)
                LOG.error(msg, e)
            }
        }
    }

    def getOrg(jsonString : String) : (String, String, Int) = {
        val parsed = parse(jsonString)
        val orgName = extractString(parsed \ "noun")
        val subreddit = extractString(parsed \ "subreddit")
        val count = extractInt(parsed \ "numNoun")

        (subreddit, orgName, count)
    }

    def getLemmas(jsonString : String) : (String, Array[String]) = {
        val parsed = parse(jsonString)
        val tokensVal = extractString(parsed \ "sentence").toLowerCase
        val tokens = tokensVal.split(" ")
        val subreddit = extractString(parsed \ "subreddit")

        (subreddit, tokens)
    }

    case class Text(text : Array[String])

    def createSynonyms(sqlContext : SQLContext, subredditName : String, orgs : RDD[(Int, String)],
        sentences : RDD[Text])(implicit config : Config) : Array[(String, String, Int, Array[(String, Double)])] = {

        import sqlContext.implicits._
        val documentDF = sentences.toDF()

        val word2Vec = new Word2Vec()
            .setInputCol("text")
            .setOutputCol("result")
            .setVectorSize(config.vectorSize)
            .setMinCount(config.minNumWords)

        val model : Word2VecModel = word2Vec.fit(documentDF)

        val collectedOrgs = orgs.collect

        collectedOrgs.map { case (count, name) => {
            try {
                println("Getting synonyms for: " + name)
                val synonymsFrame : DataFrame = model.findSynonyms(name.toLowerCase.replace(" ", "_"), config.numSynonyms)
                val synonyms : Array[(String, Double)] = synonymsFrame.collect()
                    .map { row => (row.getAs[String]("word"), row.getAs[Double]("similarity")) }

                println("Completed synonyms for: " + name + " synonyms: " + (synonyms mkString " "))
                (subredditName, name, count, synonyms)

            } catch {
                // in case not in vocabulary
                case e: Exception => {
                    val msg = s"Could not get synonym for $subredditName, $name"
                    println(msg)
                    LOG.info(msg, e)
                    (subredditName, name, count, Array[(String, Double)]())
                }
            }
        }}
    }

    def createSynonymsJson(sc : SparkContext, sqlContext : SQLContext,
        subredditName : String, orgs : RDD[(Int, String)], sentences : RDD[Text],
        outPath : String)(implicit config : Config) {

        val synonyms = createSynonyms(sqlContext, subredditName, orgs, sentences).map {
            case(subreddit, org, count, synonyms) => compact(render(
                ("subreddit" -> subreddit) ~ ("org" -> org) ~ ("count" -> count) ~
                ("synonyms" -> (synonyms.toSeq.map {
                    case (word, similarity) => { ("word" -> word) ~ ("similarity" -> similarity) }
                }))
            ))
        }

        val orgDataJson : RDD[String] = sc.parallelize(synonyms)

        save(outPath, orgDataJson)
        println("Completed subreddit: " + subredditName)
    }

    def vecForSubreddits(sc : SparkContext, sqlContext : SQLContext,
        subreddits : RDD[(String, Int)], orgsRdd : RDD[(String, String, Int)],
        lemmasRdd : RDD[(String, Array[String])], outPath : String)(implicit config : Config) {

        val filteredSubreddits : Array[(String, Int)] = subreddits
            .filter { case (subreddit, count) => (count > config.minNumSubredditComments) }
            .collect()

        filteredSubreddits.foreach {
            case (subreddit : String, count : Int) => {

                val orgs : RDD[(Int, String)] = orgsRdd
                    .flatMap { case(checkSubreddit : String, name : String, count : Int) =>
                        if (checkSubreddit == subreddit && count >= config.minOrgCount) Seq((count, name)) else Seq()
                    }

                val sentences : RDD[Text] = lemmasRdd
                    .flatMap { case(checkSubreddit : String, lemmas : Array[String]) =>
                        if (checkSubreddit == subreddit) Seq(Text(lemmas)) else Seq()
                    }

                createSynonymsJson(sc, sqlContext, subreddit, orgs, sentences, outPath + "/orgSynonyms/" + subreddit)
            }
        }
    }

    def createSubreddits(subredditsCount : RDD[(String, Int)], outPath : String)(implicit config : Config) {
        val sortedSubreddits : RDD[String] = subredditsCount
            .map(item => item.swap)
            .sortByKey(false, 1)
            .map(item => item.swap)
            .map { case(subreddit, count) => compact(render(("subreddit" -> subreddit) ~ ("commentsNum" -> count))) }

        save(outPath, sortedSubreddits)
    }

    def vecForAll(sc : SparkContext, sqlContext : SQLContext,
        orgsRdd : RDD[(String, String, Int)], lemmasRdd : RDD[(String, Array[String])],
        outPath : String)(implicit config : Config) {

        val allOrgsRdd : RDD[(Int, String)] = orgsRdd
            .map { case (subreddit, org, num) => (org, num) }
            .reduceByKey {(n1,n2) => (n1+n2)}
            .flatMap { case(name, count) => if (count >= config.minOrgCount) Seq((count, name)) else Seq() }

        val allSentences : RDD[Text] = lemmasRdd
            .map { case(checkSubreddit : String, lemmas : Array[String]) => Text(lemmas) }

        createSynonymsJson(sc, sqlContext, "all", allOrgsRdd, allSentences, outPath + "/all")
    }

    def main(args: Array[String]) {

        val inPath = args(0).toString
        val outPath = args(1).toString
        val env = args(2).toString

        println(inPath)
        println(outPath)
        println(env)

        implicit val config = if (env == "prod") ProdConfig else LocalConfig

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)

        val orgsJsonData = sc.textFile(inPath + "/orgs")
        val lemmasJsonData = sc.textFile(inPath + "/orgsSentences")

        val orgsRdd : RDD[(String, String, Int)] = orgsJsonData.map { jsonString => {
          getOrg(jsonString)
        }}

        val lemmasRdd : RDD[(String, Array[String])] = lemmasJsonData.map { jsonString => {
          getLemmas(jsonString)
        }}

        val subreddits = lemmasRdd.map { case (subreddit, lemmas) => (subreddit, 1) }
            .reduceByKey {(n1,n2) => (n1+n2)}

        createSubreddits(subreddits, outPath + "/subreddits")

        val sqlContext = new SQLContext(sc)

        vecForAll(sc, sqlContext, orgsRdd, lemmasRdd, outPath)
        vecForSubreddits(sc, sqlContext, subreddits, orgsRdd, lemmasRdd, outPath)
    }
}
