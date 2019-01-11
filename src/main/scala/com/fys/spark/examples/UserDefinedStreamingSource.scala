package com.fys.spark.examples

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object UserDefinedStreamingSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Structed Network WordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("fyssocket")
      .option("host","cmhhost1.novalocal")
      .option("port",19999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      //.format("com.fys.spark.streamingsink")
      //.format("fyssink")
      .trigger(Trigger.ProcessingTime(1000, TimeUnit.MILLISECONDS))
      .option("checkpointLocation","/tmp/temporary-" + UUID.randomUUID.toString)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
