import org.scalatest._
import VecExampleProcess._
import Matchers._

class VecExampleTest extends FlatSpec {

    it should "check text sequence" in {

        val seq : Seq[Tuple1[Array[String]]] = getSequence()
        val tup : Tuple1[Array[String]]= seq(0)
        val arr : Array[String]= tup._1

        arr{0} should equal ("Gamma")
        arr{1} should equal ("is")
        arr{2} should equal ("a")
    }
}
