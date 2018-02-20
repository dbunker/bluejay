package com.bluejay.wordcount

import com.bluejay.wordcount.SentenceParser._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.bluejay.common.Common._

object WordCountProcess {

  def count(valueSet: RDD[String]): RDD[(String, Int)] =
    valueSet
      .map(v => (v, 1))
      .reduceByKey((n1, n2) => n1 + n2)

  def sort(countSet: RDD[(String, Int)]): RDD[(String, Int)] =
    countSet
      .map(item => item.swap)
      .sortByKey(ascending = false, 1)
      .map(item => item.swap)

  private def save[T](outPath: String, data: RDD[T]) {
    println(outPath)
    data.saveAsTextFile(outPath)
  }

  // return the subreddit and tag values
  def getTagValues(jsonData: RDD[String]): RDD[(String, Seq[TagValue])] =
    jsonData.map { jsonString =>
      {
        val parsed = parse(jsonString)

        val tokensVal = extractString(parsed \ "lemmas")
        val tokens    = tokensVal.split(" ")

        val rawTokens = tokensVal.split(" ")

        val tagsVal = extractString(parsed \ "tags")
        val tags    = tagsVal.split(" ")

        val entsVal = extractString(parsed \ "ents")
        val ents    = entsVal.split(" ")

        val subreddit = extractString(parsed \ "subreddit")

        val tagSeq = Seq(tags, tokens, rawTokens, ents).transpose.map {
          case Seq(tag, value, rawValue, ent) => TagValue(tag, value, rawValue, ent)
        }

        (subreddit, tagSeq)
      }
    }

  final case class OrgSub(orgLowerCase: String, subreddit: String)
  final case class OrgCount(org: String, count: Int)
  final case class AdjCount(isDirect: Boolean, adj: String, count: Int)

  def collectAdjNoun(iter: Iterable[Option[AdjCount]], isDirect: Boolean): Seq[JObject] =
    iter.toSeq
      .flatMap {
        // isDirect is stable identifier
        case (Some(AdjCount(`isDirect`, adj, num))) => Seq((adj, num))
        case _                                      => Seq[Nothing]()
      }
      .sortWith { case ((_, c1), (_, c2)) => c1 > c2 }
      .map { case (adj, count) => ("adj" -> adj) ~ ("count" -> count) }

  // adjNouns: RDD subreddit, adjnoun
  // entCount: RDD (orgLowerCase, subreddit), (org, orgCount)
  def adjNounsJson(adjNouns: RDD[(String, AdjNoun)], entCount: RDD[(OrgSub, OrgCount)]): RDD[String] = {

    val modCounts: RDD[(OrgSub, AdjCount)] = adjNouns
      .map {
        case (subreddit, AdjNoun(isDirect, noun, adj)) => ((OrgSub(noun.toLowerCase(), subreddit), adj, isDirect), 1)
      }
      .reduceByKey { (n1, n2) =>
        n1 + n2
      }
      .map { case ((orgSub, adj, isDirect), num) => (orgSub, AdjCount(isDirect, adj, num)) }

    // filter to output nouns by joining to organizations (ordered by most occurrences), list org even without adj count
    val entJoin: RDD[(OrgSub, (OrgCount, Option[AdjCount]))] = entCount.leftOuterJoin(modCounts)

    val entJoinReset: RDD[((Int, String, String), Option[AdjCount])] = entJoin
      .map {
        case (orgSub, (orgCount, adjCount)) => ((orgCount.count, orgCount.org, orgSub.subreddit), adjCount)
      }

    entJoinReset
      .groupByKey()
      .sortByKey(ascending = false)
      .map {
        case ((num, orgName, subreddit), iter) =>
          compact(
            render(
              ("noun"         -> orgName) ~ ("subreddit" -> subreddit) ~ ("numNoun" -> num) ~
              ("directAdj"    -> collectAdjNoun(iter, isDirect = true)) ~
              ("connectedAdj" -> collectAdjNoun(iter, isDirect = false))
            ))
      }
  }

  def getOrgSentencesJson(tagsSentences: RDD[(String, Seq[TagValue])]): RDD[String] =
    tagsSentences flatMap {
      case (subreddit, sentenceSeq) =>
        val ents: Seq[(String, String)] = getEnts(sentenceSeq).map { org =>
          (subreddit, org)
        }
        if (ents nonEmpty) {
          Seq(
            compact(
              render(
                ("subreddit" -> subreddit) ~ ("sentence" -> (combineOrgs(sentenceSeq).map { tag =>
                  tag.value
                } mkString " "))
              )))
        } else Seq[Nothing]()
    }

  private def createAdjNouns(jsonData: RDD[String], outPath: String) {

    val tagsSentences: RDD[(String, Seq[TagValue])] = getTagValues(jsonData)

    val ents: RDD[(String, String)] = tagsSentences flatMap {
      case (subreddit, sentenceSeq) =>
        getEnts(sentenceSeq).map { org =>
          (subreddit, org)
        }
    }

    val entCount: RDD[(OrgSub, OrgCount)] = ents
      .map { case (subreddit, org) => (OrgSub(org.toLowerCase(), subreddit), OrgCount(org, 1)) }
      .reduceByKey { case (OrgCount(org, n1), OrgCount(_, n2)) => OrgCount(org, n1 + n2) }

    val connectedAdjNouns: RDD[(String, AdjNoun)] = tagsSentences flatMap {
      case (subreddit, sentenceSeq) =>
        getConnectedPairs(sentenceSeq).map { tag =>
          (subreddit, tag)
        }
    }

    val directAdjNouns: RDD[(String, AdjNoun)] = tagsSentences flatMap {
      case (subreddit, sentenceSeq) =>
        getDirectPairs(sentenceSeq).map { tag =>
          (subreddit, tag)
        }
    }

    val allAdjNouns     = connectedAdjNouns.union(directAdjNouns)
    val allAdjNounsJson = adjNounsJson(allAdjNouns, entCount)
    save(outPath + "/orgs", allAdjNounsJson)

    val orgSentencesJson = getOrgSentencesJson(tagsSentences)
    save(outPath + "/orgsSentences", orgSentencesJson)
  }

  def main(args: Array[String]): Unit = {

    val inPath  = args(0) toString
    val outPath = args(1) toString

    println(inPath)
    println(outPath)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc   = new SparkContext(conf)

    val jsonData = sc.textFile(inPath)
    createAdjNouns(jsonData, outPath)
  }
}
