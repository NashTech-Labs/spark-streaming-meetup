package com.knoldus.receiver


import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CustomReceiver extends App {

  val sparkConf = new SparkConf().setAppName("CustomReceiver")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val lines = ssc.receiverStream(new CustomReceiver(args(0)))
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}


class CustomReceiver(path: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    new Thread("File Reader") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {}

  private def receive() =
    try {
      println("Reading file " + path)
      val reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))
      var userInput = reader.readLine()
      while (!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      println("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case ex: Exception =>
        restart("Error reading file " + path, ex)
    }

}

