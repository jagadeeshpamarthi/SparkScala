package com.jag.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaxTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val date = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (date, entryType, temperature)
  }
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    val maxTemp = parsedLines.filter(x => x._2 == "PRCP")
    
    val finalRdd = maxTemp.map(x => (x._1,x._3.toFloat)).reduceByKey((x,y) => max(x,y))
    
    val reverse = finalRdd.map( x=> (x._2,x._1))
    
    val sorted =  reverse.sortByKey(false)
       
    sorted.collect().foreach(println)
    
  }
}