package com.fys.spark.examples

import org.apache.spark.sql.SparkSession

object SparkWordCount {

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val line = spark.sparkContext.textFile(args(0))
    line.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)
    spark.stop()
  }
}
