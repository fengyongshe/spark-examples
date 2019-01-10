package com.fys.spark.streamingsink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class DefaultSource extends StreamSinkProvider with DataSourceRegister with Logging {

  override def shortName() : String = {
    this.getClass.getName
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    println(s"FileSinkExtendProvider create FileSinkExtend")
    new FileSinkExtend(sqlContext, parameters, partitionColumns, outputMode);
  }

}
