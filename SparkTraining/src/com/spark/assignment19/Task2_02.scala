package com.spark.assignment19

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object Task2_02 {
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
    
    val groupStudents = spark.sql("SELECT grade, COUNT(*) FROM student GROUP BY grade")
    println("1 --> NUMBER OF STUDENTS BY GRADE")
    groupStudents.show()

    val avgOfStudents = spark.sql("SELECT name, grade, AVG(marks) FROM student GROUP BY name, grade")
    println("2 -->  AVERAGE OF EACH STUDENT")
    avgOfStudents.show()
    
    val avgScoreGrades = spark.sql("SELECT subject, AVG(marks) FROM student GROUP BY subject")
    println("3 --> AVERAGE SCORE OF STUDENTS IN EACH SUBJECT ACROSS ALL GRADES")
    avgScoreGrades.show()
    
    val avgScoreSubject = spark.sql("SELECT subject, grade, AVG(marks) FROM student GROUP BY subject, grade")
    println("4 --> AVERAGE SCORE OF STUDENTS IN EACH SUBJECT PER GRADE")
    avgScoreSubject.show()
    
    val avgScore = spark.sql("SELECT name, AVG(marks) FROM student WHERE grade = \"grade-2\" GROUP BY name HAVING AVG(marks) > 50")
    println("5 --> ALL STUDENTS IN GRADE-2, NUMBER OF THOSE HAVING AVERAGE SCORE GREATER THAN 50")
    avgScore.show()
  }
}