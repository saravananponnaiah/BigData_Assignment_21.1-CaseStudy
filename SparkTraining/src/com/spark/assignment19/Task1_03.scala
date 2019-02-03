package com.spark.assignment19

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Task1_03 {
  def main(args : Array[String]) : Unit = {
    
    val sc = new SparkContext("local", "Training")
    val rdd = sc.textFile("E:/Acadgild/Data/SampleData.txt")
    val tokenized = rdd.flatMap(_.split("-"))
    val wordCount = tokenized.map((_, 1)).reduceByKey(_ + _)
    val totalCount = wordCount.map(_._2).sum()
    
    println("TOTAL NUMBER OF WORDS : " + totalCount)
  }
}