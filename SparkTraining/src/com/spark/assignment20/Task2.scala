package com.spark.assignment20

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row
//import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{col, udf}

object Task2 {
  def main(args : Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder.appName("Players").master("local").getOrCreate()
    import spark.implicits._
    
    val rddSports = spark.sparkContext.textFile("E:/Acadgild/Data/Sports_data.txt")
    val sportsHeader = rddSports.first()
    val filteredRddSports = rddSports.filter(row => row != sportsHeader)
       
    val schema = StructType(sportsHeader.split(",").map(fieldName => StructField(fieldName,StringType, true)))
    val rowDD = filteredRddSports.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4),x(5),x(6)))
    val sportsDF = spark.createDataFrame(rowDD, schema)
    sportsDF.createOrReplaceTempView("sports_data")
    
    // Define UDF to form full name. Function takes input parameters as firstname and lastname
    val getFullName = (fname : String, lname : String) => {
      "Mr." + fname.take(2) + " " + lname
    }
    
    // Define UDF to get player proficiency. Input parameters as medal type and age
    val getPlayerProficiency = (medal_type : String, age : String) => {
      if (medal_type =="gold" && age.toInt >= 32)
        "Professional"
      else if (medal_type == "gold" && age.toInt <= 31)
        "Amateur"
      else if (medal_type == "silver" && age.toInt >= 32)
        "Expert"
      else if (medal_type == "silver" && age.toInt <= 31)
        "Rookie"
      else
        "Novice"
    }
    
    // Register UDF
    spark.udf.register("formFullName", getFullName)
    spark.udf.register("playerProficiency", getPlayerProficiency)
    
    val playerData = spark.sql("SELECT firstname, lastname, formFullName(firstname,lastname) AS fullname, playerProficiency(medal_type,age) AS proficiency FROM sports_data")
    playerData.show()
    
  }
}