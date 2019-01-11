package org.apache.spark.sql.execution.streaming.streamingsource

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.Socket
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}
import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

class TextSocketSource(host: String, port: Int, includeTimestamp: Boolean, sqlContext: SQLContext)
  extends Source with Logging {

  @GuardedBy("this")
  private var socket: Socket = null

  @GuardedBy("this")
  private var readThread: Thread = null

  @GuardedBy("this")
  protected val batches = new ListBuffer[(String,Timestamp)]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1L)

  @GuardedBy("this")
  protected var lastOffsetCommitted: LongOffset = new LongOffset(-1L)

  initialize()

  private def initialize(): Unit = {
    socket = new Socket(host,port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

    readThread = new Thread(s"TextSocketSource($host,$port)") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          val line = reader.readLine()
          if (line == null) {
            logWarning(s"Stream closed by $host:$port")
            return
          }
          TextSocketSource.this.synchronized {
            val newData = (
              line,
              Timestamp.valueOf(
                TextSocketSource.DATE_FORMAT.format(Calendar.getInstance().getTime)
              )
            )
            currentOffset += 1
            batches.append(newData)
          }
        } catch {
          case e: IOException =>
        }
      }
    }

    readThread.start()
  }

  override def schema: StructType = {
    if (includeTimestamp) TextSocketSource.SCHEMA_TIMESTAMP
    else TextSocketSource.SCHEMA_REGULAR
  }

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1){
      None
    } else {
      Some(currentOffset)
    }
  }

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startOrdinal =
      start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    val rdd = sqlContext.sparkContext
      .parallelize(rawList)
      .map { case (v, ts) => InternalRow(UTF8String.fromString(v), ts.getTime) }

    sqlContext.internalCreateDataFrame(rdd,schema,isStreaming = true)

  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    if (socket != null) {
      try {
        // Unfortunately, BufferedReader.readLine() cannot be interrupted, so the only way to
        // stop the readThread is to close the socket.
        socket.close()
      } catch {
        case e: IOException =>
      }
      socket = null
    }
  }

  override def toString: String = s"TextSocketSource[host: $host, port: $port]"

}

object TextSocketSource {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}
