package com.fys.spark.examples

import org.apache.spark.sql.SparkSession

object SQLExamplesSource {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources Example")
      .getOrCreate()

    runBasicDataSource(spark)
    runParquetExample(spark)
    runJsonDatasetExample(spark)

    spark.close()
  }

  private def runBasicDataSource(spark: SparkSession): Unit = {
    val usersDF = spark.read.load("/data/resources/users.parquet")

    usersDF.show()

    usersDF.select("name","favorite_color").write.save("namesAndFavColors.parquet")
    usersDF.write.format("orc")
      .option("orc.bloom.filter.columns","favorite_color")
      .option("orc.dictionary.key.threshold","1.0")
      .option("orc.column.encoding.direct","name")
      .save("users_with_options.orc")

    val peopleDF = spark.read.format("json").load("/data/resources/people.json")
    peopleDF.select("name","age").write.format("parquet").save("namesAndAges.parquet")

    val peopleDFCsv = spark.read.format("csv")
      .option("seq",";")
      .option("inferSchema","true")
      .option("header","true")
      .load("/data/resources/people.csv")

    val sqlDF = spark.sql("SELECT * from parquet.`/data/resources/users.parquet`")

    peopleDF.write.bucketBy(42,"name").sortBy("age").saveAsTable("people_bucketed")

    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    usersDF.write.partitionBy("favorite_color")
      .bucketBy(42,"name")
      .saveAsTable("users_partitioned_bucketed")

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")

  }

  private def runParquetExample(spark: SparkSession): Unit = {

    import spark.implicits._
    val peopleDF = spark.read.json("/data/resources/people.json")
    peopleDF.write.parquet("people.parquet")

    val parquetFileDF = spark.read.parquet("people.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")
    parquetFileDF.printSchema()
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()

  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {

    import spark.implicits._

    val path = "/data/resources/people.json"
    val peopleDF = spark.read.json(path)

    peopleDF.printSchema()
    peopleDF.createOrReplaceTempView("people")

    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 and 19")
    teenagerNamesDF.show()

    val otherPeopleDataset = spark.createDataset( """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()

  }
}
