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

    // Steps
    // 1. Filter by size
    // 2. Filter by unique letter (loses vs loose) and filter by possible starting letters ???
    // 3. Create letter count for each word in dictionary

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Jumble Solver")
      .getOrCreate()
    import spark.implicits._

    // Read in JSON and convert into RDD[String]
    val lines = spark.sparkContext.textFile("./src/main/resources/freq_dict.json")
    val count = lines.count()
    var linesIdx = lines.zipWithIndex().collect{  // Omit first and last lines of file
      case (v, index) if index != 0 && index != count-1 => v
    }
    linesIdx = linesIdx.map(_.replaceAll(" |\"|,",""))  // Omit the characters ' ', ',', '"'

    // Split lines by ':' into RDD[String,String] and turn into RDD[WordCount]
    val jsonRDD = linesIdx.map(_.split(":"))
    val freqRDD = jsonRDD.map(wc => WordCount(wc(0), wc(1).toInt))

    // Convert RDD[WordCount] to DataFrame
    val freqDF = freqRDD.toDF()


    // De-Jumble word
    val word = "ramoj"

//    val result = freqDF.filter(length($"word")===word.length)
//    result.show(10)


    val sortCharactersUDF = udf[String,String](sortCharacters)

    val freqWithSortedCharsDF = freqDF.withColumn("sortedChars", sortCharactersUDF($"word"))


    val sortedCharArray = word.toCharArray
    Arrays.sort(sortedCharArray)
    val sortedWord = new String(sortedCharArray)

    val result = freqWithSortedCharsDF.filter($"sortedChars"===sortedWord)
    result.cache()

    result.show(10)

    val rankedSolnsDF = result.filter($"frequency" > 0)
    val numRankedSolns = rankedSolnsDF.count()

    val unrankedSolnsDF = result.filter($"frequency"===0)
    val numUnrankedSolns = unrankedSolnsDF.count()

    if(numRankedSolns > 0) {
      val answer = rankedSolnsDF
        .orderBy(asc("frequency"))
        .first()
        .getAs[String]("word")
      println(answer)
    }
    else {
      val answer = unrankedSolnsDF
        .first()
        .getAs[String]("word")
      println(answer)
    }

//    val rankedFreqDF = result.filter($"frequency" > 0).orderBy(asc("frequency"))
//
//    val unrankedFreqDF = result.filter($"frequency"===0)
//    unrankedFreqDF.show(10)
//    val answer = unrankedFreqDF.first().getAs[String]("word")
//    println(answer)

  }

  // TODO USE CASES:
  // - Choose smallest freq >0 (with no 0s)
  // - Choose smallest freq >0 (even with 0s)
  // - If only 0s, pick any

  // HELPER FUNCTIONS

  // Sort chars in word
  def sortCharacters(word: String): String ={
    val chars = word.toCharArray
    Arrays.sort(chars)
    return new String(chars)
  }
}


//// Sort chars in word
//val sortCharArray: String => String = {
//  word => {
//  var temp = word.toCharArray
//  Arrays.sort(temp)
//  new String(temp)
//}
//}

