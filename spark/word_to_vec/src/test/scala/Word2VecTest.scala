import org.scalatest._
import Word2VecProcess._
import Matchers._

class Word2VecTest extends FlatSpec {

    it should "get organization and lemmas from string" in {

      val orgJson = "{ \"noun\": \"Camero\", \"subreddit\": \"cars\", \"numNoun\": 2 }"
      val (orgSubreddit, orgName, count) : (String, String, Int) = getOrg(orgJson)

      (orgSubreddit, orgName, count) should equal ("cars", "Camero", 2)

      val lemmasJson = "{ \"sentence\":\"the sport car be red .\", \"subreddit\": \"cars\" }"
      val (lemmasSubreddit, tokens) : (String, Array[String]) = getLemmas(lemmasJson)

      lemmasSubreddit should equal ("cars")
      tokens should equal (Array("the", "sport", "car", "be", "red", "."))
    }
}
