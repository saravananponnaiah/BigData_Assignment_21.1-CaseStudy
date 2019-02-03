package com.spark.assignment21

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object Task1 {
  def main(args : Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder.appName("Students").master("local").getOrCreate()
    import spark.implicits._
    
    val rddSports = spark.sparkContext.textFile("E:/Acadgild/Data/Sports_data.txt")
    val sportsHeader = rddSports.first()
    val filteredRddSports = rddSports.filter(row => row != sportsHeader)
       
    val schema = StructType(sportsHeader.split(",").map(fieldName => StructField(fieldName,StringType, true)))
    val rowDD = filteredRddSports.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4),x(5),x(6)))
    val sportsDF = spark.createDataFrame(rowDD, schema)
    sportsDF.createOrReplaceTempView("sports_data")

    println("1 --> What are the total number of gold medal winners every year")
    val goldWinners = spark.sql("SELECT year, COUNT(*) AS medal_count FROM sports_data WHERE trim(medal_type)=\"gold\" GROUP BY year ORDER BY year ASC")
    goldWinners.show()
 
    println("2 --> How many silver medals have been won by USA in each sport")
    val medalCount = spark.sql("SELECT sports, COUNT(*) FROM sports_data WHERE country=\"USA\" AND medal_type=\"silver\" GROUP BY sports")
    medalCount.show()
    
    spark.stop()
  }
}