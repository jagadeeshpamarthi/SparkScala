package com.jag.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object CustomerAmount {
   def parseLine(line:String)= {
    val fields = line.split(",")
    val custId = fields(0).toInt
    val productId = fields(1).toInt
    val amountSpent = fields(2).toFloat
    (custId, amountSpent)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerAmount")
    
    // Read each line of input data
    val lines = sc.textFile("../customer-orders.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    val groupRDD = parsedLines.reduceByKey(_+_)
    val reverse = groupRDD.map(x => (x._2,x._1))
    
    val sortRDD = reverse.sortByKey(false)
    val allValues = sortRDD.collect()
    for(x <- allValues){
      val amount = x._1
      val cust = x._2
      val amt = f"$amount%.2f"
     println(s"$cust,$amt")
    }
    
  }
}