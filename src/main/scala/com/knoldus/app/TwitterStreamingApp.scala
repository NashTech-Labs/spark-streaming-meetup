package com.knoldus.app

import com.knoldus.nlp.SentimentAnalyzer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status


object TwitterStreamingApp extends App {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterStreamingApp")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  val filters = List("startbucks", "sbux", "startbuck",
    "coffestartbuck", "coffee", "StarbucksUK", "StarbucksCanada", "StarbucksMY",
    "StarbucksIndia", "StarbucksIE", "StarbucksAu", "StarbucksFrance", "StarbucksMex", "StarBucksTweet")
  val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, filters)//.window(Seconds(5*10))
  val tweets: DStream[String] = stream.map(_.getText).map(tweet => SentimentAnalyzer.getSentiment(tweet))


  val pairs: DStream[(String, Int)] = tweets.map(word => (word, 1))

  val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

  wordCounts.print(10)

  ssc.start()
  ssc.awaitTermination()


}

