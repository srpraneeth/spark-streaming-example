package com.travelogue.services.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import com.fasterxml.jackson.databind.ObjectMapper

import com.travelogue.services.streaming.dto.SimplePost
import scala.reflect.ClassTag
import com.travelogue.services.streaming.dto.SimplePost

object App {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val objectMapper = new ObjectMapper
    val posts = RabbitMQUtils.createStream(ssc, Map[String, String]("host" -> "localhost", "queueName" -> "postsQueue"), bytes => objectMapper.readValue(bytes, classOf[SimplePost]) )


    /*val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1))
    val wordsCountReduced = wordCounts.reduceByKey(_ + _)
    wordsCountReduced.print()*/

    ssc.start()
    ssc.awaitTermination()
  }
}