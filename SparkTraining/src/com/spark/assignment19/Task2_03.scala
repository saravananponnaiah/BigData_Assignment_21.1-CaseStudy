package com.spark.assignment19

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object Task2_03 {
  def main(args : Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder.appName("Students").master("local").getOrCreate()
    import spark.implicits._
    
    val rddStudent = spark.sparkContext.textFile("E:/Acadgild/Data/19_Dataset.txt")
    
    // Defining the data-frame header structure
    val studentHeader = "name subject grade marks age"
    
    // It uses library org.apache.spark.sql.types.{StructType, StructField, StringType}
    val schema = StructType(studentHeader.split(" ").map(fieldName => StructField(fieldName,StringType, true)))
    
    // Defining the row Dataframe
    val rowDD = rddStudent.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4)))

    // Create Dataframe
    val studentDF = spark.createDataFrame(rowDD, schema)
    
    studentDF.createOrReplaceTempView("student")
    
    println("Average score per student_name across all grades is same as average score per student_name per grade")
    val avgPerStudAllGrades = spark.sql("SELECT name, AVG(marks) FROM student GROUP BY name")
    val avgPerStudPerGrade = spark.sql("SELECT name, AVG(marks) FROM student GROUP BY name, grade")
    
    avgPerStudAllGrades.show()
    avgPerStudPerGrade.show()
    
    println("INTERSECTION OF DATAFRAMES")
    val resultIntersect = avgPerStudAllGrades.intersect(avgPerStudPerGrade)
    if (resultIntersect.count() == 0) {
      println("There is no common average score in two data frames")
    }
    else {
      resultIntersect.show()
    }
    
  }
}