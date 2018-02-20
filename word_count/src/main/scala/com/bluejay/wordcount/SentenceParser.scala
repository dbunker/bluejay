package com.bluejay.wordcount

import com.bluejay.common.Common._

object SentenceParser {

  val empty: Seq[String] = Seq[String]()

  final case class TagValue(tag: String, value: String, rawValue: String, ent: String)
  final case class AdjNoun(isDirect: Boolean, noun: String, adj: String)
  final case class NounNoun(noun: String, descriptionNoun: String)

  implicit class Regex(sc: StringContext) {
    def r: util.matching.Regex = new util.matching.Regex(sc.parts.mkString, sc.parts.drop(1).map(_ => "x"): _*)
  }

  // examples:
  // DT RB JJ NN VBZ RB JJ CC RB RB JJ
  // the very large car is somewhat red and also not green

  // DT JJ RB JJ NNS NN VBZ RB JJ CC RB RB JJ
  // the large very purple sports car is somewhat red and also not green

  final case class DirectAcc(modList: Seq[String], adjList: Seq[String], nounList: Seq[String], adjNouns: Seq[AdjNoun])

  // of the general form: adjective (JJ) noun (NN)
  def getDirectPairs(sentenceSeq: Seq[TagValue]): Seq[AdjNoun] =
    sentenceSeq
      .foldLeft(DirectAcc(empty, empty, empty, Seq[AdjNoun]())) {

        case (DirectAcc(modList, adjList, nounList, adjNouns), tagValue) =>
          val TagValue(tag, value, _, _) = tagValue
          val reset                      = DirectAcc(empty, empty, empty, adjNouns)

          tag match {

            case r"RB.*" => DirectAcc(modList :+ value, adjList, empty, adjNouns)
            case r"JJ.*" => DirectAcc(empty, adjList :+ (modList :+ value).mkString(" "), empty, adjNouns)
            case r"CC.*" => DirectAcc(empty, adjList, empty, adjNouns)

            case r"NN.*" => DirectAcc(empty, adjList, nounList :+ value, adjNouns)

            case _ =>
              if (adjList.nonEmpty && nounList.nonEmpty) {
                val fullNoun = nounList.mkString(" ")
                val newAdjNouns = adjList.map { adjValue =>
                  AdjNoun(isDirect = true, fullNoun, adjValue)
                }
                DirectAcc(empty, empty, empty, adjNouns ++ newAdjNouns)
              } else reset
          }
      }
      .adjNouns

  final case class ConnectedAcc(modList: Seq[String],
                                adjList: Seq[String],
                                nounList: Seq[String],
                                sawVerb: Boolean,
                                adjNouns: Seq[AdjNoun])

  // of the general form: noun (NN) be (VB) adjective (JJ)
  def getConnectedPairs(sentenceSeq: Seq[TagValue]): Seq[AdjNoun] =
    sentenceSeq
      .foldLeft(ConnectedAcc(empty, empty, empty, sawVerb = false, Seq[AdjNoun]())) {

        case (ConnectedAcc(modList, adjList, nounList, sawVerb, adjNouns), tagValue) =>
          val TagValue(tag, value, _, _) = tagValue
          val reset                      = ConnectedAcc(empty, empty, empty, sawVerb = false, adjNouns)

          (sawVerb, tag) match {

            case (false, r"NN.*") => ConnectedAcc(empty, empty, nounList :+ value, sawVerb = false, adjNouns)

            case (false, r"VB.*") =>
              (value, nounList.length) match {
                case (_, 0)    => reset
                case ("be", _) => ConnectedAcc(empty, empty, nounList, sawVerb = true, adjNouns)
                case _         => reset
              }

            // articles
            case (true, r"((IN)|(DT)).*") => ConnectedAcc(modList, adjList, nounList, sawVerb = true, adjNouns)

            // modifier to adjective
            case (true, r"RB.*") => ConnectedAcc(modList :+ value, adjList, nounList, sawVerb = true, adjNouns)
            case (true, r"JJ.*") =>
              ConnectedAcc(empty, adjList :+ (modList :+ value).mkString(" "), nounList, sawVerb = true, adjNouns)
            case (true, r"CC.*") => ConnectedAcc(empty, adjList, nounList, sawVerb = true, adjNouns)

            case (true, _) =>
              if (nounList.nonEmpty) {
                val fullNoun: String = nounList.mkString(" ")
                val possibleDescriptorNoun: Seq[String] = tag match {
                  case r"NN.*" => Seq(value)
                  case _       => Seq[Nothing]()
                }

                val newAdjNouns: Seq[AdjNoun] = adjList.map { adjValue =>
                  AdjNoun(isDirect = false, fullNoun, (Seq(adjValue) ++ possibleDescriptorNoun) mkString " ")
                }

                ConnectedAcc(empty,
                             empty,
                             empty,
                             sawVerb = false,
                             adjNouns ++ newAdjNouns ++
                             possibleDescriptorNoun.map { value =>
                               AdjNoun(isDirect = false, fullNoun, value)
                             })
              } else reset

            case _ => reset
          }
      }
      .adjNouns

  final case class EntTagAcc(valuesAcc: Seq[String], valueSoFar: String, lastEnt: String)

  // output all organizations in a list of tag values (words)
  def getEnts(sentenceSeq: Seq[TagValue]): Seq[String] =
    sentenceSeq
      .foldLeft(EntTagAcc(Seq[String](), "", "")) {
        case (EntTagAcc(valuesAcc, valueSoFar, lastEnt), TagValue(_, value, _, ent)) =>
          if (ent != "ORGANIZATION") {
            EntTagAcc(if (valueSoFar === "") valuesAcc else valuesAcc :+ valueSoFar, "", ent)
          } else if (lastEnt === lastEnt) {
            EntTagAcc(valuesAcc, valueSoFar + (if (valueSoFar === "") "" else " ") + value, ent)
          } else {
            EntTagAcc(valuesAcc, value, ent)
          }
      }
      .valuesAcc

  def combineOrgs(sentenceSeq: Seq[TagValue]): Seq[TagValue] =
    sentenceSeq.foldRight(Seq[TagValue]())(
      (tagValue, tagSeq) =>
        if (tagValue.ent === "ORGANIZATION" && tagSeq.nonEmpty && tagSeq { 0 }.ent === "ORGANIZATION")
          TagValue(tagValue.tag,
                   tagValue.value + "_" + tagSeq { 0 }.value,
                   tagValue.rawValue + "_" + tagSeq { 0 }.rawValue,
                   tagValue.ent) +: tagSeq.drop(1)
        else
          tagValue +: tagSeq
    )
}
