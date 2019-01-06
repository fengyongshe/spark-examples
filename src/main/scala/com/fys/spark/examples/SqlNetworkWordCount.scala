package com.fys.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SqlNetworkWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <Hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SQLNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD {
      (rdd: RDD[String], time: Time) => {

        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._

        val worddsDataFrame = rdd.map( w => Record(w)).toDF()
        worddsDataFrame.createOrReplaceTempView("words")

        val wordCountsDataFrame = spark.sql("SELECT word, count(*) as total from words group by word")
        println(s"============== $time =================")
        wordCountsDataFrame.show()
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}

case class Record(word: String)

object SparkSessionSingleton  {

  @transient private var instance : SparkSession = _

  def getInstance(sparkConf: SparkConf) : SparkSession = {

    if (instance == null) {
      instance = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }

}
