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

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object App {
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
    //wordCounts.print()

    words.foreachRDD { rdd =>

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // Register as table
      wordsDataFrame.registerTempTable("words")

      // Do word count on DataFrame using SQL and print it
      //val wordCountsDataFrame =
      //  sqlContext.sql("select word, count(*) as total from words group by word")
       val wordCountsDataFrame =
        sqlContext.sql("select word from words")
      //wordCountsDataFrame.write.mode("append").format("parquet").save("namesAndAges.parquet")
      wordCountsDataFrame.write.mode("append").text("path2.txt")
      
    }

    ssc.start()
    ssc.awaitTermination()
  }
}