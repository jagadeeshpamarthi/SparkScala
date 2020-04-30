package com.jag.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import javassist.expr.Cast
import java.util.Date

object ChicagoCrimeRecord {
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("ChicagoCrimeRecord")
      .master("local[*]")
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    //val lines = spark.sparkContext.textFile("../chicagoCrimeRecord.csv")
    //lines.take(10).foreach(println)
    val crimeDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load("../chicagoCrimeRecord.csv")
    //crimeDF.printSchema()
    //crimeDF.show()
    //crimeDF.select("Primary Type").distinct().collect().foreach(println)
    //crimeDF.select("ID").filter($"Latitude" =!= null).collect().foreach(println)
    val crimeTypeDate = crimeDF.select($"Date",$"Primary Type")
    val crimeType = crimeTypeDate.withColumnRenamed("Primary Type", "Primary_Type")
    crimeType.printSchema()
    //crimeTypeDate.show()
   //crimeTypeDate.groupBy($"Date",$"Primary Type").count().orderBy($"Date" asc, $"Primary Type" desc).show()
    crimeType.createOrReplaceTempView("crimeData")
    val crimePerMonthPerType = spark.sql("select concat(substr(Date,7,4),substr(Date,1,2)) as crime_date, count(1) as count, Primary_Type " +
              "from crimedata group by concat(substr(Date,7,4),substr(Date,1,2)), Primary_Type order by crime_date, count desc ")
    crimePerMonthPerType.coalesce(1).write.format("csv").option("codec", "gzip")save("../crimePerMonthPerType2")
    print("done")
    spark.stop()
  }
}