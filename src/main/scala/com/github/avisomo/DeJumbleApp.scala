package com.github.avisomo

import java.util
import java.util.Arrays
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * @author ${user.name}
 */
object DeJumbleApp {

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

    // TODO Solve final Jumble
    dejumbler.solveCartoon(circleLetters, circleWordSizes)

    // lnd vs dnl would provide us same token "dln"; thus use permutations instead of combinations
    // first sort
    // then find permutation (token) for 3 size, 4 size, and 4 size

    // Method 2
    // For each word token, filter out those whose letters are not in this circle letters list
    // Combine each  3 size word with a 4 size and a 4 size
    // and each 4 size word with a 3 size and 4 size
    // tokenize them and select the ones that match
    // Choose the triplet that contains the most precise frequency

  }
}

