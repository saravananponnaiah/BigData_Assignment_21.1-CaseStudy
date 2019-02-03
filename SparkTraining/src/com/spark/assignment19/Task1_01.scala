package com.spark.assignment19

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// Scala program to find the number of lines in input file
object Task1_01 {
  def main(args : Array[String]) : Unit = {
    val sc = new SparkContext("local","Word Count")
    // Read a text file and create a RDD
    val rdd = sc.textFile("E:/Acadgild/Data/AndroidBarCode.txt")

    var lineCount = rdd.count()
    
    println("LINE COUNT IN FILE : " + lineCount)
    println("--------------------------------------")
    
  }
}