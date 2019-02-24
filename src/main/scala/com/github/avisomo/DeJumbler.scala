package com.github.avisomo

import java.util.Arrays

import com.github.avisomo.App.sortCharacters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeJumbler(spark: SparkSession, jsonPath: String) extends Serializable {

  import spark.implicits._

  // TODO Validate on persistence strategy

  // cache freq Dataframe (NOT in dejumble)
  val jsonRDD = readFreqDictAsRDD(jsonPath)
  val freqDF = createFrequencyDF(jsonRDD)

  // For each word in freqDF, generate a token that will be used to identify possible solutions
  val tokenizedFreqDF = tokenizeWords(freqDF)
  tokenizedFreqDF.cache()


  def dejumble(targetWord: String): Unit ={

    val targetToken = sortCharacters(targetWord)

    // filter for possible words
    val solnsFreqDF = tokenizedFreqDF.filter($"token"===targetToken)

    // TODO choose most likely word

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


  // Sort chars in word
  def sortCharacters(word: String): String ={
    val chars = word.toCharArray
    Arrays.sort(chars)

    new String(chars)
  }

}
