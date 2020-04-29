package com.jag.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByFirstName {
  
  def lines(line : String)={
    val fields = line.split(",")
    val firstName = fields(1)
    val numFriends = fields(3).toInt
    (firstName,numFriends)
  }
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","FriendsByFirstName")
    val data = sc.textFile("../fakefriends.csv")
    val rdd = data.map(lines)
    val result = rdd.mapValues(x=>(x,1)).reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
    val avgFriends = result.map(x => (x._1 , x._2._1 / x._2._2))
    val resultSorted = avgFriends.sortByKey(true)
    resultSorted.collect().foreach(println)
    
  }
}