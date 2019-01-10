package com.fys.spark.streamingsink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

class FileSinkExtend(sQLContext: SQLContext,
                     parameters: Map[String,String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode ) extends Sink with Logging {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    println(s"FileSinkExtend add batch. data:")
    val rows = data.collect()
    println(s"Result Row length:" + rows.length)
    rows.take(10).foreach(row => println(s"row is :" + row.toString))

  }
}
