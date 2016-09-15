package com.knoldus.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Streaming2App extends App {

  val checkpointDirectory = "checkpointDir" //It should be fault-tolerant & reliable file system (e.g., HDFS, S3, etc.)

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingApp")

  sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

  val streamingContext = new StreamingContext(sparkConf, Seconds(5))
  streamingContext.checkpoint(checkpointDirectory)

  val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9000)

  val words: DStream[String] = lines.flatMap(_.split(" "))

  val filteredWords: DStream[String] = words.filter(!_.trim.isEmpty)

  val pairs: DStream[(String, Int)] = filteredWords.map(word => (word, 1))

  val updatedState: DStream[(String, Int)] =
    pairs.updateStateByKey[Int] {
      (newValues: Seq[Int], state: Option[Int]) =>
        Some(newValues.sum +state.getOrElse(0))
    }


  updatedState.print(20)

  streamingContext.start()
  streamingContext.awaitTermination()

}

