package com.spark.assignment19

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// Program to calculate the number of words in the input file
object Task1_02 {
  def main(args : Array[String]) : Unit = {
    val sc = new SparkContext("local","Word Count")
    // Read a text file and create a RDD
    val rdd = sc.textFile("E:/Acadgild/Data/AndroidBarCode.txt")

    // Split the document content in to words
    val tokenized = rdd.flatMap(_.split(" "))
    
    // Count the occurence of each word
    val wordCount = tokenized.map((_, 1)).reduceByKey(_ + _)
    
    println(wordCount.collect().mkString(", "))
    println("--------------------------------------")
    
  }
}