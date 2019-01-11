package com.fys.spark.examples

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaStreamingWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Structed Network WordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","cmhhost1.novalocal:6667")
      .option("startingoffset","smallest")
      .option("subscribe","words")
      .load()

    val query = lines
          .writeStream
          .format("console")
          .outputMode("append")
          .start()

    query.awaitTermination()

  }

}
