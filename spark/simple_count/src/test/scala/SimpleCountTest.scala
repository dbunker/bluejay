import org.scalatest._
import SimpleCountProcess._
import Matchers._

class SimpleCountTest extends FlatSpec {

    it should "convert json to only body text" in {

        val jsonString = "{ \"body\": \"body text\", \"other\": 5, \"test\": \"other text\" }"

        val body = getBody(jsonString)
        body should equal ("body text")
    }
}
