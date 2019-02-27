package com.github.avisomo

import java.util.Arrays

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{asc, length, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeJumbler(spark: SparkSession, jsonPath: String) extends Serializable {

  import spark.implicits._

  val jsonRDD: RDD[Array[String]] = readFreqDictAsRDD(jsonPath)
  val freqDF: DataFrame = createFrequencyDF(jsonRDD)

  // For each word in freqDF, generate a token that will be used to identify possible solutions
  val tokenizedFreqDF: DataFrame = tokenizeWords(freqDF)
  tokenizedFreqDF.cache()


  // Dejumble the provided worc for the best possible answer
  def dejumble(targetWord: String): String ={

    val targetToken = sortCharacters(targetWord)

    // filter for possible words
    val solnsFreqDF = tokenizedFreqDF.filter($"token"===targetToken)

    // return most likely word as solution
    chooseBestSoln(solnsFreqDF)
  }

  // TODO Create test class
  def solveCartoon(circleLetters: List[Char], circleWordSizes: List[Int]): Unit ={
    val word1Size = 3
    val word2Size = 4
    val word3Size = 4

//    tokenizedFreqDF.select("word")



//    var wordComboDF = tokenizedFreqDF.select("word").where(length($"word")===3 || length($"word")===4)
//    wordComboDF.show(10)

    // List(l, n, d, j, o, b, e, a, l, w, e)
    // Add column that shows  letters from provided circle letters list that are not in "word"
    // job | lndealwe
    // lob | ndjealwe
    // tob | ---


    // job | lend | alwe
    // lob | deal | njwe


    // job | lend | weal
    // job | deal | ----

    // Iteratively identify the first 3-letter words with remainder column, then identify second 4-letter
    // words with remainder column, lastly identify last 4-letter column. For each end of iteration,
    // filter out remainders of "---" or "----"

    var wordComboDF = tokenizedFreqDF
      .select("word")
      .where(length($"word")===word1Size)
    wordComboDF.show(10)





//    val temp = tokenizedFreqDF.filter(length($"word")===word2Size)
//    temp.show(10)
//    wordComboDF = wordComboDF.withColumn(
//      "word2",
//      temp.col("word"))
//
//    wordComboDF = wordComboDF.withColumn(
//      "word3",
//      tokenizedFreqDF.filter(length($"word")===word3Size).col("word"))
//
////    for (i <- 1 until circleWordSizes.length) {
////
////    }
//    wordComboDF.show(10)

  }


  // HELPER FUNCTIONS


  // Convert multiline JSON file of key:value pairs into RDD[key,value]
  def readFreqDictAsRDD(path: String): RDD[Array[String]] ={
    val lines = spark.sparkContext.textFile(path)
    lines.cache() // Save time from Spark actions like count(), collect(), etc.

    val count = lines.count()
    var linesIdx = lines.zipWithIndex().collect{  // Omit first and last lines of file
      case (v, index) if index != 0 && index != count-1 => v
    }
    linesIdx = linesIdx.map(_.replaceAll(" |\"|,",""))  // Omit the characters ' ', ',', '"'

    // Split lines by ':' into RDD[String,String] and turn into RDD[WordCount]
    val keyValRDD = linesIdx.map(_.split(":"))

    lines.unpersist()
    keyValRDD
  }


  // Convert RDD[String,String] to DataFrame
  def createFrequencyDF(KeyValRDD: RDD[Array[String]]): DataFrame ={
    val freqRDD = KeyValRDD.map(wc => WordCount(wc(0), wc(1).toInt))

    freqRDD.toDF()
  }

  // Add new column "token"
  def tokenizeWords(freqDF: DataFrame): DataFrame ={
    val sortCharactersUDF = udf[String,String](sortCharacters)
    val tokenizedFreqDF = freqDF.withColumn("token", sortCharactersUDF($"word"))

    tokenizedFreqDF
  }


//  def filterOutStrFromCharSet(str: String, charSet: List[Char]): List[Char] ={
//
//  }

  // TODO: Allow multiple string arguments, but first make sure the word combos DF doesn't take too long to create
  // Sort chars in word
  def sortCharacters(word: String): String ={
    val chars = word.toCharArray
    Arrays.sort(chars)

    new String(chars)
  }

  // Given a word frequency DataFrame, choose the best possible answer
  def chooseBestSoln(solnsFreqDF: DataFrame): String = {
    solnsFreqDF.cache()

    val rankedSolnsDF = solnsFreqDF.filter($"frequency" > 0)
    val numRankedSolns = rankedSolnsDF.count()

    val unrankedSolnsDF = solnsFreqDF.filter($"frequency" === 0)
    val numUnrankedSolns = unrankedSolnsDF.count()

    if (numRankedSolns > 0) {
      val answer = rankedSolnsDF
        .orderBy(asc("frequency"))
        .first()
        .getAs[String]("word")

      solnsFreqDF.unpersist()
      answer
    }
    else {
      val answer = unrankedSolnsDF
        .first()
        .getAs[String]("word")

      solnsFreqDF.unpersist()
      answer
    }
  }

}
