package com.fys.spark.examples

package com.fys.spark.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

case class Record(key: Int, value: String)

object SparkSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark RDD Examples")
      .config("spark.config.option", "value")
      .getOrCreate()

    import spark.implicits._
    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    df.createOrReplaceTempView("records")

    println("Result of SELECT *")
    spark.sql("SELECT * FROM records").collect().foreach(println)

    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    val rddFromSQL = spark.sql("SELECT key, value FROM records WHERE key < 10")
    println("Result of RDD.map:")
    rddFromSQL.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    val parquetFile  = spark.read.parquet("pair.parquet")
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)

    spark.stop()

  }

}