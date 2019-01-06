package com.fys.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWordCount {

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("Usage: WordCount <directory>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("HDFS WordCount!")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x ,1 )).reduceByKey(_ + _)
    wordCounts.print
    ssc.start()
    ssc.awaitTermination()
  }

}

