package com.spark.assignment21

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object TemperatureCaseStudy {
  def main(args : Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Creating a spark context object to run in local machine
    val spark = SparkSession.builder.appName("TemperatureController").master("local").getOrCreate()
    import spark.implicits._
    
    // Create a dataframe and load the building csv file
    val dfBuildings = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("E:/Acadgild/Data/building.csv")
    
      // Create a dataframe and load the hvac csv file
    val dfHvac = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("E:/Acadgild/Data/HVAC.csv")
      
    dfBuildings.printSchema()
    dfHvac.printSchema()
    
    // UDF to determine whether there is a significant variance in temperature 
    val isTemperatureChange = (target : Int, actual : Int) => {
      if ((target - actual) > 5 || (target - actual) < -5)
        1
      else
        0
    }
    
    // Register UDF isTemperatureChange
    spark.udf.register("isTempChange", isTemperatureChange)
    
    // Add new column TempChange and flag as 1 or 0 based on the temperature variance condition 
    val dfNewHvac = dfHvac.withColumn("TempChange", when( ($"TargetTemp" - $"ActualTemp") > 5 || ($"TargetTemp" - $"ActualTemp") < -5, 1  ).otherwise(0))
    
    // Create temporary tables for dataframes
    dfBuildings.createOrReplaceTempView("buildings")
    dfNewHvac.createOrReplaceTempView("hvac")
    
    val result = spark.sql("SELECT b.Country, COUNT(h.TempChange) AS OccurenceCount FROM buildings AS b INNER JOIN hvac AS h ON b.BuildingID = h.BuildingID WHERE h.TempChange = 1 GROUP BY b.Country")
    result.show()
    
    spark.stop()
  }
}