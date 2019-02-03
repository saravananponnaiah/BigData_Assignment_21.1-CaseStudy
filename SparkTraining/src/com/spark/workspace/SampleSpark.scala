package com.spark.workspace
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SampleSpark {
  def main(args : Array[String]) : Unit = {
    
    val sc = new SparkContext("local", "Training")
    val rdd = sc.textFile("E:/Acadgild/Data/SampleData.txt")
    val tokenized = rdd.flatMap(_.split(" "))
    val wordCount = tokenized.map((_, 1)).reduceByKey(_ + _)
    //val totalCount = wordCount.reduce((a, b) => (a + b))
    val totalCount = wordCount.map(_._2).sum()
    
    println("TOTAL NUMBER OF WORDS : " + totalCount)
  }
}