package com.knoldus.kafka


import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._


object KafkaDirectStreaming extends App {

  val brokers = "localhost:9092"

  val sparkConf = new SparkConf().setAppName("KafkaDirectStreaming").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  ssc.checkpoint("checkpointDir")

  val topicsSet = Set("streaming_queue1")
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages: InputDStream[(String, String)] =
    KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


  val lines = messages.map { case (key, message) => message }
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}



