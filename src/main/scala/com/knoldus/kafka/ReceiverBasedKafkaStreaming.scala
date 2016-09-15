package com.knoldus.kafka

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ReceiverBasedStreaming extends App {

  val group = "streaming-test-group"
  val zkQuorum = "localhost:2181"

  val topics = Map("streaming_queue" -> 1) // topic, numbers of threads

  val checkpointDirectory = "checkpointDir"

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiverBasedStreamingApp")
  sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  ssc.checkpoint(checkpointDirectory)

  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topics,StorageLevel.MEMORY_ONLY).map { case (key, message) => message }
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()

}


/***

# Create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic streaming_queue

# producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streaming_queue

  */