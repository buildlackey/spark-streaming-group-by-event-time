package org.apache.spark.sql.execution.streaming

// CLONE
//
// This file contains a copy of org.apache.spark.sql.execution.streaming.TextSocketSource
// modified to emit more logging information in the methods initialize(), getOffset() and getBatch().
// So that the additional logging does not overly slow down processing we don't print the content of every event
// received over the socket right away. We put those into a buffer, whose contents we can retrieve all
// in one shot via getMsgs().
//
// Note that we use the same package name as the original DataSource because the original classes accessed
// some methods that were only visible in the scope of this package.
//
// Our streaming job will use this modified class if we specify its name as an argument to format() when
// constructing a stream reader, as in:
//
//    sparkSession.readStream.format("org.apache.spark.sql.execution.streaming.TextSocketSourceProvider2")

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.Socket
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object TextSocketSource2 {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)

  val msgs: ListBuffer[String] = mutable.ListBuffer[String]()   // CLONE
  def getMsgs(): List[String] = msgs.toList                     // CLONE
}

/**
 * A source that reads text lines through a TCP socket, designed only for tutorials and debugging.
 * This source will *not* work in production applications due to multiple reasons, including no
 * support for fault recovery and keeping all of the text read in memory forever.
 */
class TextSocketSource2(host: String, port: Int, includeTimestamp: Boolean, sqlContext: SQLContext)
  extends Source with Logging {


  @GuardedBy("this")
  private var socket: Socket = null

  @GuardedBy("this")
  private var readThread: Thread = null

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[(String, Timestamp)]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  initialize()

  private def initialize(): Unit = synchronized {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    readThread = new Thread(s"TextSocketSource($host, $port)") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          while (true) {
            val line: String = reader.readLine()
            TextSocketSource2.msgs += s"socket read at ${new Date().toString} line:" + line
            //System.out.println(s"socket read at ${new Date().toString} line:" + line);     // CLONE
            if (line == null) {
              // End of file reached
              logWarning(s"Stream closed by $host:$port")
              return
            }
            TextSocketSource2.this.synchronized {
              val newData = (line,
                Timestamp.valueOf(
                  TextSocketSource2.DATE_FORMAT.format(Calendar.getInstance().getTime()))
                )
              currentOffset = currentOffset + 1
              batches.append(newData)
            }
          }
        } catch {
          case e: IOException =>
        }
      }
    }
    readThread.start()
  }

  /** Returns the schema of the data from this source */
  override def schema: StructType = if (includeTimestamp) TextSocketSource2.SCHEMA_TIMESTAMP
  else TextSocketSource2.SCHEMA_REGULAR

  override def getOffset: Option[Offset] = synchronized {
    val retval = if (currentOffset.offset == -1) {
      None
    } else {
      Some(currentOffset)
    }
    println(s" at ${new Date().toString} getOffset: " + retval)     // CLONE
    retval
  }

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    println(s" at ${new Date().toString} getBatch start:" + start  + ". end: " + end)     // CLONE

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
      .map {
        case (v, ts) =>
          //println(s" to row at ${new Date().toString} $v")
          InternalRow(UTF8String.fromString(v), ts.getTime)
      }
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
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

class TextSocketSourceProvider2 extends StreamSourceProvider with DataSourceRegister with Logging {
  private def parseIncludeTimestamp(params: Map[String, String]): Boolean = {
    Try(params.getOrElse("includeTimestamp", "false").toBoolean) match {
      case Success(bool) => bool
      case Failure(_) =>
        throw new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
    }
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!parameters.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!parameters.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    if (schema.nonEmpty) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }

    val sourceSchema =
      if (parseIncludeTimestamp(parameters)) {
        TextSocketSource2.SCHEMA_TIMESTAMP
      } else {
        TextSocketSource2.SCHEMA_REGULAR
      }
    ("textSocket", sourceSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    new TextSocketSource2(host, port, parseIncludeTimestamp(parameters), sqlContext)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket2"    // CLONE
}

