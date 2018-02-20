package com.bluejay.wordtovec

import java.io._

import org.apache.log4j.{ BasicConfigurator, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.ml.feature.{ Word2Vec, Word2VecModel }
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.bluejay.common.Common._

object Word2VecProcess {
  private val LOG = Logger.getLogger(this.getClass.getSimpleName)

  abstract class Config extends Serializable {
    val vectorSize: Int  = 300
    val numSynonyms: Int = 10
    val minNumWords: Int
    val minNumSubredditComments: Int
    val minOrgCount: Int
  }

  object ProdConfig extends Config {
    override val minNumWords             = 10
    override val minNumSubredditComments = 5000
    override val minOrgCount             = 40
  }

  object LocalConfig extends Config {
    override val minNumWords             = 0
    override val minNumSubredditComments = 30
    override val minOrgCount             = 3
  }

  private def save[T](outPath: String, data: RDD[T]) {
    println("Write to: " + outPath)
    try {
      data.saveAsTextFile(outPath)
    } catch {
      // in case cannot write file
      case e: Exception =>
        val msg = s"Could not write to $outPath"
        println(msg)
        LOG.error(msg, e)
    }
  }

  def getOrg(jsonString: String): (String, String, Int) = {
    val parsed    = parse(jsonString)
    val orgName   = extractString(parsed \ "noun")
    val subreddit = extractString(parsed \ "subreddit")
    val count     = extractInt(parsed \ "numNoun")

    (subreddit, orgName, count)
  }

  def getLemmas(jsonString: String): (String, Seq[String]) = {
    val parsed    = parse(jsonString)
    val tokensVal = extractString(parsed \ "sentence").toLowerCase
    val tokens    = tokensVal.split(" ")
    val subreddit = extractString(parsed \ "subreddit")

    (subreddit, tokens)
  }

  final case class Text(text: Seq[String])

  private def createSynonyms(config: Config,
                             sparkSession: SparkSession,
                             subredditName: String,
                             orgs: RDD[(Int, String)],
                             sentences: RDD[Text]): Seq[(String, String, Int, Seq[(String, Double)])] = {

    import sparkSession.implicits._
    val documentDF = sentences.toDF()

    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(config.vectorSize)
      .setMinCount(config.minNumWords)

    val model: Word2VecModel = word2Vec.fit(documentDF)

    val collectedOrgs = orgs.collect

    collectedOrgs.map {
      case (count, name) =>
        try {
          println("Getting synonyms for: " + name)
          val synonymsFrame: DataFrame = model.findSynonyms(name.toLowerCase.replace(" ", "_"), config.numSynonyms)
          val synonyms: Seq[(String, Double)] = synonymsFrame
            .collect()
            .map { row =>
              (row.getAs[String]("word"), row.getAs[Double]("similarity"))
            }

          println("Completed synonyms for: " + name + " synonyms: " + (synonyms mkString " "))
          (subredditName, name, count, synonyms)

        } catch {
          // in case not in vocabulary
          case e: Exception =>
            val msg = s"Could not get synonym for $subredditName, $name"
            println(msg)
            LOG.info(msg, e)
            (subredditName, name, count, Seq[(String, Double)]())
        }
    }
  }

  private def createSynonymsJson(config: Config,
                                 sc: SparkContext,
                                 sparkSession: SparkSession,
                                 subredditName: String,
                                 orgs: RDD[(Int, String)],
                                 sentences: RDD[Text],
                                 outPath: String) {

    val synonyms: Seq[String] = createSynonyms(config, sparkSession, subredditName, orgs, sentences).map {
      case (subreddit, org, count, commentSynonyms) =>
        compact(
          render(
            ("subreddit" -> subreddit) ~ ("org" -> org) ~ ("count" -> count) ~
            ("synonyms" -> commentSynonyms.map {
              case (word, similarity) => ("word" -> word) ~ ("similarity" -> similarity)
            })
          ))
    }

    val orgDataJson: RDD[String] = sc.parallelize(synonyms, sc.defaultParallelism)

    save(outPath, orgDataJson)
    println("Completed subreddit: " + subredditName)
  }

  private def vecForSubreddits(config: Config,
                               sc: SparkContext,
                               sparkSession: SparkSession,
                               subreddits: RDD[(String, Int)],
                               orgsRdd: RDD[(String, String, Int)],
                               lemmasRdd: RDD[(String, Seq[String])],
                               outPath: String) {

    val filteredSubreddits: Seq[(String, Int)] = subreddits
      .filter { case (_, count) => count > config.minNumSubredditComments }
      .collect()

    filteredSubreddits.foreach {
      case (subreddit: String, _: Int) =>
        val orgs: RDD[(Int, String)] = orgsRdd
          .flatMap {
            case (checkSubreddit: String, name: String, count: Int) =>
              if (checkSubreddit === subreddit && count >= config.minOrgCount) Seq((count, name)) else Seq[Nothing]()
          }

        val sentences: RDD[Text] = lemmasRdd
          .flatMap {
            case (checkSubreddit: String, lemmas: Seq[String]) =>
              if (checkSubreddit === subreddit) Seq(Text(lemmas)) else Seq[Nothing]()
          }

        createSynonymsJson(config, sc, sparkSession, subreddit, orgs, sentences, outPath + "/orgSynonyms/" + subreddit)
    }
  }

  private def createSubreddits(config: Config, subredditsCount: RDD[(String, Int)], outPath: String) {
    val sortedSubreddits: RDD[String] = subredditsCount
      .map(item => item.swap)
      .sortByKey(ascending = false, 1)
      .map(item => item.swap)
      .map { case (subreddit, count) => compact(render(("subreddit" -> subreddit) ~ ("commentsNum" -> count))) }

    save(outPath, sortedSubreddits)
  }

  private def vecForAll(config: Config,
                        sc: SparkContext,
                        sparkSession: SparkSession,
                        orgsRdd: RDD[(String, String, Int)],
                        lemmasRdd: RDD[(String, Seq[String])],
                        outPath: String) {

    val allOrgsRdd: RDD[(Int, String)] = orgsRdd
      .map { case (_, org, num) => (org, num) }
      .reduceByKey { (n1, n2) =>
        n1 + n2
      }
      .flatMap { case (name, count) => if (count >= config.minOrgCount) Seq((count, name)) else Seq[Nothing]() }

    val allSentences: RDD[Text] = lemmasRdd
      .map { case (_: String, lemmas: Seq[String]) => Text(lemmas) }

    createSynonymsJson(config, sc, sparkSession, "all", allOrgsRdd, allSentences, outPath + "/all")
  }

  def main(args: Array[String]): Unit = {

    val inPath  = args(0).toString
    val outPath = args(1).toString
    val env     = args(2).toString

    println(inPath)
    println(outPath)
    println(env)

    val config: Config = if (env === "prod") ProdConfig else LocalConfig

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc   = new SparkContext(conf)

    val orgsJsonData   = sc.textFile(inPath + "/orgs")
    val lemmasJsonData = sc.textFile(inPath + "/orgsSentences")

    val orgsRdd: RDD[(String, String, Int)] = orgsJsonData.map { jsonString =>
      {
        getOrg(jsonString)
      }
    }

    val lemmasRdd: RDD[(String, Seq[String])] = lemmasJsonData.map { jsonString =>
      {
        getLemmas(jsonString)
      }
    }

    val subreddits = lemmasRdd
      .map { case (subreddit, _) => (subreddit, 1) }
      .reduceByKey { (n1, n2) =>
        n1 + n2
      }

    createSubreddits(config, subreddits, outPath + "/subreddits")

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    vecForAll(config, sc, sparkSession, orgsRdd, lemmasRdd, outPath)
    vecForSubreddits(config, sc, sparkSession, subreddits, orgsRdd, lemmasRdd, outPath)
  }
}
