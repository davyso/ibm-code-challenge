package com.github.avisomo

import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def main(args: Array[String]) {

    val sc = new SparkContext( "local", "Word Count", "/usr/local/spark")

    val input = sc.parallelize(Seq("a","a","b"))
    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("outfile")
    System.out.println("OK");
  }

}
