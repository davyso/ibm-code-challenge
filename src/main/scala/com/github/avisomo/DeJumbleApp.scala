package com.github.avisomo

import org.apache.spark.sql.SparkSession

/**
 * @author avisomo
 */
object DeJumbleApp {

  // Demo run of first four Jumbles in Puzzle 1
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Jumble Solver")
      .getOrCreate()

    val dejumbler = new DeJumbler(spark, "./src/main/resources/freq_dict.json")
    val jumbles = List(
      "nagld",
      "ramoj",
      "camble",
      "wraley"
    )

    val circleIdxs = List(
      List(1,3,4),
      List(2,3),
      List(0,1,3),
      List(0,2,4)
    )

    val circleWordSizes = List(3,4,4)

    // Solve first four Jumbles
    val answers = jumbles.map(dejumbler.dejumble)
    println(answers)


    val circleLetters = (answers,circleIdxs).zipped.flatMap((a,ci) => {
      ci.map(a.charAt)
    })

    println(circleLetters)
    // List(l, n, d, j, o, b, e, a, l, w, e)

    // TODO: Brute force method attempt to get list of possible word combinations as solutions
    // NOTE: Feel free to uncomment to show intermedite DataFrame that was reached before acknowledging the issue
//    dejumbler.solveCircles(circleLetters, circleWordSizes)

    // TODO: Attempt to improve performance of finding possible word combinations as solution
    // NOTE: Feel free to uncomment to show intermedite DataFrame that was reached before acknowledging the issue
//    dejumbler.solveCircles2(circleLetters, circleWordSizes)

  }
}

