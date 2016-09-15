package com.knoldus.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingApp extends App {
  import RecoverableWordCount._

  val streamingContext = StreamingContext.getOrCreate(checkpointDirectory, createContext _)

  streamingContext.start()

  streamingContext.awaitTermination()
}

object RecoverableWordCount {

  val checkpointDirectory = "checkpointDir" // should a fault-tolerant, reliable file system (e.g., HDFS, S3, etc.)

  def createContext() = {

    val sparkConf = new SparkConf().setAppName("StreamingApp")
    //sparkConf.set()
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    streamingContext.checkpoint(checkpointDirectory)

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9000)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val filteredWords: DStream[String] = words.filter(!_.trim.isEmpty)

    val pairs: DStream[(String, Int)] = filteredWords.map(word => (word, 1))

    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    wordCounts.print(20)

    streamingContext
  }

}