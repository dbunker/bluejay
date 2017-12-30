import org.scalatest._
import NLPProcess._
import Matchers._

class NLPTest extends FlatSpec {

    it should "tag tokens in text" in {

        val sentence = "The large and very purple sports car is somewhat red and also not green. The plump singing duck was hungry."

        val (tokenData, lemmaData, tagData, entData) = tagTokens(sentence)

        tokenData should equal ("The large and very purple sports car is somewhat red and also not green . The plump singing duck was hungry .")
        lemmaData should equal ("the large and very purple sport car be somewhat red and also not green . the plump singing duck be hungry .")
        tagData should equal ("DT JJ CC RB JJ NNS NN VBZ RB JJ CC RB RB JJ . DT JJ NN NN VBD JJ .")
        entData should equal ("O O O O O O O O O O O O O O O O O O O O O O")
    }
}
