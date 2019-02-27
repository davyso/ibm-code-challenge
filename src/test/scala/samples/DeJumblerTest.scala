package samples

import org.apache.spark.sql.SparkSession

import scala.collection._
import org.scalatest.{Assertions, BeforeAndAfter, FunSuite}
import org.junit.Test
import com.github.avisomo.DeJumbler

class DeJumblerTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before{
    spark = SparkSession
      .builder()
      .master("local")
      .appName("DeJumbler Unit Testing")
      .getOrCreate()
  }

  test("sortCharacters - correctly sort"){
    val dejumbler = new DeJumbler(spark, "./src/main/resources/freq_dict.json")
    val actual = dejumbler.sortCharacters("moon")
    assert(actual.equals("mnoo"))
  }

  after{
    spark.stop()
  }


}
