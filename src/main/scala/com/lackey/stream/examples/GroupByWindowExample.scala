package com.lackey.stream.examples

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.TextSocketSource2
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object GroupByWindowExample {

  case class AnimalView(timeSeen: Timestamp, animal: String, howMany: Integer)

  val fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger(classOf[MicroBatchExecution]).setLevel(Level.DEBUG)

    import sparkSession.implicits._

    val socketStreamDs: Dataset[String] =
      sparkSession.readStream
        .format("org.apache.spark.sql.execution.streaming.TextSocketSourceProvider2")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
        .as[String] // Each string in dataset == 1 line sent via socket

    val writer: DataStreamWriter[Row] = {
      getDataStreamWriter(sparkSession, socketStreamDs)
    }

    Future {
      Thread.sleep(4 * 5 * 1000)
      val msgs = TextSocketSource2.getMsgs()
      System.out.println("msgs:" + msgs.mkString("\n"))
    }

    writer
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()

  }

  def getDataStreamWriter(sparkSession: SparkSession,
                          lines: Dataset[String])
  : DataStreamWriter[Row] = {

    import sparkSession.implicits._

    // Process each line in 'lines' by splitting on the "," and creating
    // a Dataset[AnimalView] which will be partitioned into 5 second window
    // groups. Within each time window we sum up the occurrence counts
    // of each animal .
    val animalViewDs: Dataset[AnimalView] = lines.flatMap {
      line => {
        try {
          val columns = line.split(",")
          val str = columns(0)
          val date: Date = fmt.parse(str)
          Some(AnimalView(new Timestamp(date.getTime), columns(1), columns(2).toInt))
        } catch {
          case e: Exception =>
            println("ignoring exception : " + e);
            None
        }
      }
    }

    val windowedCount = animalViewDs
      //.withWatermark("timeSeen", "5 seconds")
      .groupBy(
      window($"timeSeen", "5 seconds"), $"animal")
      .sum("howMany")

    windowedCount.writeStream
      .trigger(Trigger.ProcessingTime(5 * 1000))
  }
}
