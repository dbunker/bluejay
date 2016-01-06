import org.scalatest._
import SentenceParser._
import Matchers._

class SentenceParserTest extends FlatSpec {

    it should "generate direct adjective noun pairs" in {

        val tags = "DT JJ CC RB JJ NNS NN VBZ RB JJ CC RB RB JJ . DT JJ NN NN VBD JJ ." split " "
        val tokens = "the large and very purple sports car be somewhat red and also not green . the plump singing duck was hungry ." split " "

        val pairs = (tags zip tokens).map { case(tag, value) => {
            TagValue(tag, value, null, null)
        }}

        val adjNouns = getDirectPairs(pairs)

        assert(adjNouns contains AdjNoun(true, "sports car", "large"))
        assert(adjNouns contains AdjNoun(true, "sports car", "very purple"))
        assert(adjNouns contains AdjNoun(true, "singing duck", "plump"))
        assert(adjNouns.size == 3)
    }

    it should "generate connected adjective noun pairs" in {

        val tags = "DT JJ CC RB JJ NNS NN VBZ RB JJ CC RB RB JJ . DT JJ NN NN VBD JJ ." split " "
        val tokens = "the large and very purple sports car be somewhat red and also not green . the plump singing duck be hungry ." split " "

        val pairs = (tags zip tokens).map { case(tag, value) => {
            TagValue(tag, value, null, null)
        }}

        val adjNouns = getConnectedPairs(pairs)

        assert(adjNouns contains AdjNoun(false, "sports car", "somewhat red"))
        assert(adjNouns contains AdjNoun(false, "sports car", "also not green"))
        assert(adjNouns contains AdjNoun(false, "singing duck", "hungry"))
        assert(adjNouns.size == 3)
    }

    it should "generate list of organizations" in {

        val entTags = "O O ORGANIZATION ORGANIZATION O O O O ORGANIZATION ORGANIZATION O O O O O ORGANIZATION ORGANIZATION O" split " "
        val tokens = "I think Alpha Corp. be lacking substance , Alpha Corp. needs restructuring , as does Gama Corp. ." split " "

        val pairs = (entTags zip tokens).map { case(tag, value) => {
            TagValue(null, value, null, tag)
        }}

        val orgs = getEnts(pairs)

        orgs should contain theSameElementsAs List("Gama Corp.", "Alpha Corp.", "Alpha Corp.")
    }

    it should "combine orgs" in {

        val entTags = "O O ORGANIZATION ORGANIZATION O O O O ORGANIZATION ORGANIZATION O O O O O ORGANIZATION ORGANIZATION O" split " "
        val tokens = "I think Alpha Corp. be lacking substance , Alpha Corp. needs restructuring , as does Gama Corp. ." split " "

        val pairs = (entTags zip tokens).map { case(tag, value) => {
            TagValue(null, value, value, tag)
        }}

        val combined = combineOrgs(pairs)
        assert(combined contains TagValue(null, "Alpha_Corp.", "Alpha_Corp.", "ORGANIZATION"))
    }
}
