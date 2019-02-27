package com.github.avisomo

import java.util.Arrays

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{asc, array, col, collect_list, length, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class DeJumbler(spark: SparkSession, jsonPath: String) extends Serializable {

  import spark.implicits._

  val jsonRDD: RDD[Array[String]] = readFreqDictAsRDD(jsonPath)
  val freqDF: DataFrame = createFrequencyDF(jsonRDD)

  // For each word in freqDF, generate a token that will be used to identify possible solutions
  val tokenizedFreqDF: DataFrame = tokenizeWords(freqDF)
  tokenizedFreqDF.cache()


  // Dejumble the provided worc for the best possible answer
  def dejumble(targetWord: String): String ={

    val targetToken = sortCharacters(List(targetWord))

    // filter for possible words
    val solnsFreqDF = tokenizedFreqDF.filter($"token"===targetToken)

    // return most likely word as solution
    chooseBestSoln(solnsFreqDF)
  }

  // Obtain possible solutions based on word size and create a 'supertoken' based on them. Then, generate a target
  // supertoken from the provided circle letters and filter for this from the possible solutions DataFrame. Lastly,
  // for each possible solution, measure frequency rank and return the lowest cumulative rank
  def solveCircles(circleLetters: List[Char], circleWordSizes: List[Int]): Unit ={
    val sortCharactersUDF = udf[String,List[String]](sortCharacters)

    var possibleSolnsDF = tokenizedFreqDF
      .select($"word".alias("word0"))
      .where(length(col("word0"))===circleWordSizes(0))

    for(i <- 1 until circleWordSizes.length){
      val otherWords = tokenizedFreqDF
        .select($"word".alias("word"+i))
        .where(length(col("word"+i))===circleWordSizes(i))
      possibleSolnsDF = possibleSolnsDF.crossJoin(otherWords)
    }

    // Create supertoken column
    val colNames = possibleSolnsDF.columns.toSeq
    val firstColName = colNames.head
    val otherColNames = colNames.slice(1, colNames.length)

    possibleSolnsDF = possibleSolnsDF.withColumn(
      "supertoken",
      sortCharactersUDF(array(firstColName,otherColNames:_*))
    )

    possibleSolnsDF.show()

    // TODO: Bottom snippet takes too long since each node will have to interact with every other node [O(n^2)???]
//    val targetSupertoken = sortCharacters(List(circleLetters.mkString))
//    possibleSolnsDF.filter($"supertoken"===targetSupertoken).show()
  }


  // TODO: Attempt to improve performance of finding possible word cobinations as solution
  // Logic:
  // Provided circle letters: List(l, n, d, j, o, b, e, a, l, w, e)
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
  def solveCircles2(circleLetters: List[Char], circleWordSizes: List[Int]): Unit ={
    val word1Size = 3
    val word2Size = 4
    val word3Size = 4


    val filterOutStrFromCharSetUDF =  udf[String, String, String](filterOutStrFromCharSet)

//    tokenizedFreqDF.select("word")


//    var wordComboDF = tokenizedFreqDF.select("word").where(length($"word")===3 || length($"word")===4)
//    wordComboDF.show(10)


    var wordComboDF = tokenizedFreqDF
      .select("word")
      .where(length($"word")===word1Size)

    var temp = wordComboDF.withColumn(
      "remaining",
      filterOutStrFromCharSetUDF($"word", lit(circleLetters.mkString))
    )

    temp = temp.filter($"remaining".isNotNull)

    temp = temp.groupBy("remaining").agg(collect_list("word")).alias("word1")

    temp.show(10)

    val wordComboDF2 = tokenizedFreqDF.select("word").where(length($"word")===word2Size)
    wordComboDF2.crossJoin(temp).show()





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
    val sortCharactersUDF = udf[String,List[String]](sortCharacters)
    val tokenizedFreqDF = freqDF.withColumn("token", sortCharactersUDF(array($"word")))

    tokenizedFreqDF
  }


  def filterOutStrFromCharSet(str: String, charSet: String): String ={

    var remaining = charSet

    for(c <- str.toCharArray){
      if(!(remaining contains c)) return null
      remaining = remaining.replaceFirst(c.toString,"")
    }
    remaining
  }

  // TODO: Allow multiple string arguments, but first make sure the word combos DF doesn't take too long to create
  // Sort chars in word
  def sortCharacters(words: Seq[String]): String ={
    val chars = words.flatMap(_.toCharArray).toArray
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
