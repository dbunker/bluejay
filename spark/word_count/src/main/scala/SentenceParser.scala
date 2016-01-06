
object SentenceParser {

    val empty = Array[String]()

    case class TagValue(tag:String, value:String, rawValue:String, ent:String)
    case class AdjNoun(isDirect: Boolean, noun:String, adj:String)
    case class NounNoun(noun:String, descriptionNoun:String)

    implicit class Regex(sc: StringContext) {
        def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

    // examples:
    // DT RB JJ NN VBZ RB JJ CC RB RB JJ
    // the very large car is somewhat red and also not green

    // DT JJ RB JJ NNS NN VBZ RB JJ CC RB RB JJ
    // the large very purple sports car is somewhat red and also not green

    case class DirectAcc(modList:Array[String], adjList:Array[String], nounList:Array[String], adjNouns:Array[AdjNoun])

    // of the general form: adjective (JJ) noun (NN)
    def getDirectPairs (sentenceArray : Array[TagValue]) : Array[AdjNoun] = {

        sentenceArray.foldLeft(
            DirectAcc(empty, empty, empty, Array[AdjNoun]())) {

            case (DirectAcc(modList, adjList, nounList, adjNouns), tagValue) => {

                val TagValue(tag, value, rawValue, ent) = tagValue
                val reset = DirectAcc(empty, empty, empty, adjNouns)

                tag match {

                    case r"RB.*" => DirectAcc(modList :+ value, adjList, empty, adjNouns)
                    case r"JJ.*" => DirectAcc(empty, adjList :+ (modList :+ value).mkString(" "), empty, adjNouns)
                    case r"CC.*" => DirectAcc(empty, adjList, empty, adjNouns)

                    case r"NN.*" => DirectAcc(empty, adjList, nounList :+ value, adjNouns)

                    case _ => if (adjList.size > 0 && nounList.size > 0) {
                            val fullNoun = nounList.mkString(" ")
                            val newAdjNouns = adjList.map { adjValue => AdjNoun(true, fullNoun, adjValue)  }
                            DirectAcc(empty, empty, empty, adjNouns ++ newAdjNouns)
                        }
                        else reset
                }
            }
        }.adjNouns
    }

    case class ConnectedAcc(modList:Array[String], adjList:Array[String], nounList:Array[String], sawVerb:Boolean, adjNouns:Array[AdjNoun])

    // of the general form: noun (NN) be (VB) adjective (JJ)
    def getConnectedPairs (sentenceArray : Array[TagValue]) : Array[AdjNoun] = {

        sentenceArray.foldLeft(
            ConnectedAcc(empty, empty, empty, false, Array[AdjNoun]())) {

            case (ConnectedAcc(modList, adjList, nounList, sawVerb, adjNouns), tagValue) => {

                val TagValue(tag, value, rawValue, ent) = tagValue
                val reset = ConnectedAcc(empty, empty, empty, false, adjNouns)

                (sawVerb, tag) match {

                    case (false, r"NN.*") => ConnectedAcc(empty, empty, nounList :+ value, false, adjNouns)

                    case (false, r"VB.*") => (value, nounList.size) match {
                        case (_, 0) => reset
                        case ("be", _) => ConnectedAcc(empty, empty, nounList, true, adjNouns)
                        case _ => reset
                    }

                    // articles
                    case (true, r"((IN)|(DT)).*") => ConnectedAcc(modList, adjList, nounList, true, adjNouns)

                    // modifier to adjective
                    case (true, r"RB.*") => ConnectedAcc(modList :+ value, adjList, nounList, true, adjNouns)
                    case (true, r"JJ.*") => ConnectedAcc(empty, adjList :+ (modList :+ value).mkString(" "), nounList, true, adjNouns)
                    case (true, r"CC.*") => ConnectedAcc(empty, adjList, nounList, true, adjNouns)

                    case (true, _) => {
                        if (nounList.size > 0) {
                            val fullNoun : String = nounList.mkString(" ")
                            val possibleDescriptorNoun : Seq[String] = tag match { case r"NN.*" => Seq(value) case _ => Seq() }

                            val newAdjNouns : Seq[AdjNoun] = adjList.map {
                                adjValue => AdjNoun(false, fullNoun, (Seq(adjValue) ++ possibleDescriptorNoun) mkString " ")
                            }

                            ConnectedAcc(empty, empty, empty, false, adjNouns ++ newAdjNouns ++
                                possibleDescriptorNoun.map {value => AdjNoun(false, fullNoun, value)} )
                        }
                        else reset
                    }

                    case _ => reset
                }
            }
        }.adjNouns
    }

    case class EntTagAcc(valuesAcc : Seq[String], valueSoFar : String, lastEnt : String)

    // output all organizations in a list of tag values (words)
    def getEnts(sentenceArray : Array[TagValue]) : Seq[String] = {

        sentenceArray.foldLeft(EntTagAcc(Seq[String](), "", "")) {
            case (EntTagAcc(valuesAcc, valueSoFar, lastEnt), TagValue(tag, value, rawValue, ent)) => {

                if (ent != "ORGANIZATION") {
                    EntTagAcc(if (valueSoFar == "") valuesAcc else valuesAcc :+ valueSoFar, "", ent)
                } else if (lastEnt == lastEnt) {
                    EntTagAcc(valuesAcc, valueSoFar + (if (valueSoFar == "") "" else " ") + value, ent)
                } else {
                    EntTagAcc(valuesAcc, value, ent)
                }
            }
        }.valuesAcc
    }

    def combineOrgs(sentenceArray : Array[TagValue]) : Array[TagValue] = {
        sentenceArray.foldRight(Array[TagValue]()) (
            (tagValue, tagArray) => (
                if (tagValue.ent == "ORGANIZATION" && tagArray.size > 0 && tagArray.head.ent == "ORGANIZATION")
                    TagValue(tagValue.tag, tagValue.value + "_" + tagArray.head.value,
                        tagValue.rawValue + "_" + tagArray.head.rawValue, tagValue.ent) +: tagArray.tail
                    else
                    tagValue +: tagArray
            )
        )
    }
}
