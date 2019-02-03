package com.spark.assignment19

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object Task2_01 {
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
    
    val rowCount = spark.sql("SELECT COUNT(*) FROM student")    // Get number of rows in the input file
    val distinctSubjects = spark.sql("SELECT DISTINCT(subject) FROM student")
    val studentsCount = spark.sql("SELECT COUNT(*) FROM student WHERE name = \"Mathew\" AND marks = \"55\"")
    
    rowCount.map(x => "NUMBER OF ROWS : " + x(0)).show()
    println("NUMBER OF DISTINCT SUBJECTS : " + distinctSubjects.count())
    println("FILTERED STUDENT COUNT WHOSE NAME IS MATHEW AND MARKS 55")
    studentsCount.map(x => "RESULT : " + x(0)).show()

    
  }
}