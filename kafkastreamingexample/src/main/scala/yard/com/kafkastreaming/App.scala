/**
 * pass the following params
 * zookeeper_url:port consumer_group topic number_of_threads
 * localhost:2181 group  test  5
 * for example zookeeper_url:port = localhost:2181
 * consumer group = group
 * topic: test
 * number_of_threads = 5
 * 
 */

package yard.com.kafkastreaming

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

/**
 * @author ${user.name}
 */
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


object App{
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}