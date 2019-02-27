package samples

import org.apache.spark.sql.SparkSession

import scala.collection._
import org.scalatest.{Assertions, BeforeAndAfter, FunSuite}
import org.junit.Test
import com.github.avisomo.DeJumbler

class DeJumblerTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _
  var dejumbler: DeJumbler = _


  before{
    spark = SparkSession
      .builder()
      .master("local")
      .appName("DeJumbler Unit Testing")
      .getOrCreate()

    dejumbler = new DeJumbler(spark,"./src/main/resources/freq_dict.json"    )
  }

  // ==============
  // SOLVE JUMBLES
  // ==============
  test("Puzzle 1"){
    val jumbles = List(
      "nagld",
      "ramoj",
      "camble",
      "wraley"
    )
    val answers = jumbles.map(dejumbler.dejumble)
    println(answers)
  }
  test("Puzzle 2"){
    val jumbles = List(
      "bnedl",
      "idova",
      "seheyc",
      "aracem"
    )
    val answers = jumbles.map(dejumbler.dejumble)
    println(answers)
  }
  test("Puzzle 3"){
    val jumbles = List(
      "shast",
      "doore",
      "ditnic",
      "catili"
    )
    val answers = jumbles.map(dejumbler.dejumble)
    println(answers)
  }
  test("Puzzle 4"){
    val jumbles = List(
      "knidy",
      "legia",
      "cronee",
      "tuvedo"
    )
    val answers = jumbles.map(dejumbler.dejumble)
    println(answers)
  }
  test("Puzzle 5"){
    val jumbles = List(
      "gyrint",
      "drivet",
      "snamea",
      "ceedit",
      "sowdah",
      "elchek"
    )
    val answers = jumbles.map(dejumbler.dejumble)
    println(answers)
  }



  // ==============
  // UNIT TESTS
  // ==============

  test("sortCharacters - correctly sort single word"){
    val actual = dejumbler.sortCharacters(Seq("moon"))
    val expected = "mnoo"

    assert(actual.equals(expected))
  }

  test("sortCharacters - correctly sort multiple word"){
    val actual = dejumbler.sortCharacters(Seq("moon", "job"))
    val expected = "bjmnooo"

    assert(actual.equals(expected))
  }

  test("filterOutStrFromCharSet - correctly filter from list"){

    val l = List('l', 'n', 'd', 'j', 'o', 'b', 'e', 'a', 'l', 'w', 'e').mkString

    val actual = dejumbler.filterOutStrFromCharSet("eel", l).toSeq.sorted
    val expected = List('l', 'n', 'd', 'j', 'o', 'b', 'a', 'w').mkString.toSeq.sorted

    println(actual)
    println(expected)

    assert(actual.equals(expected))
  }

  after{
    spark.stop()
  }


}
