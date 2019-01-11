package org.apache.spark.sql.execution.streaming.streamingsource

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

class TextSocketSourceProvider extends StreamSourceProvider
               with DataSourceRegister with Logging {


  private def parseIncludeTimestamp(params: Map[String,String]) : Boolean = {

    Try(params.getOrElse("includeTimestamp", "false").toBoolean) match {
      case Success(bool) => bool
      case Failure(_) =>
        //throw new IOException("IncludeTimestamp must be set to either true or fasle")
        throw new AnalysisException("includeTimestamp must be set to either true or false")
    }
  }

  override def sourceSchema( sqlContext: SQLContext,
                           schema: Option[StructType],
                           providerName: String ,
                           parameters: Map[String,String]): (String,StructType) = {
    logWarning("The Socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!parameters.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\",...).")
    }
    if (!parameters.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\",...).")
    }
    if (schema.nonEmpty) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }
    val sourceSchema =
      if (parseIncludeTimestamp(parameters)) {
        TextSocketSource.SCHEMA_TIMESTAMP
      } else {
        TextSocketSource.SCHEMA_REGULAR
      }
    ("textSocket", sourceSchema)
  }

  override def createSource( sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String,String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    new TextSocketSource(host,port,parseIncludeTimestamp(parameters),sqlContext)
  }

  override def shortName(): String = "fyssocket"

}
