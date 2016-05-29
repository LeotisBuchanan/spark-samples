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

import java.util.regex.Pattern
import java.util.regex.Matcher
import kafka.serializer.StringDecoder

import Utilities._

/**
 * @author ${user.name}
 */
import java.util.HashMap

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object App {
  def main(args: Array[String]) {
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = List("testLogs").toSet
    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a 
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    //convert each log line to a row rdd 

    // Extract the (URL, status, user agent) we want from each log line
    val logs = lines.map(x => {
      val temp = x.split(",").toList;
      //list[1,2,4]
      val logTuple = temp match {
        case List(ip, datetime, requestMethod) => (ip, datetime, requestMethod)
      }

      //return the logtuple
      logTuple

    })

    logs.foreachRDD((rdd, time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      if (!rdd.isEmpty()) {
        // Convert RDD[String] to RDD[case class] to DataFrame
        val logsDataFrame = rdd.map(w => Log(w._1, w._2, w._3)).toDF()
        logsDataFrame.show()
        //save logs 
        logsDataFrame.write.mode("append").format("parquet").save("logs.parquet")
      }

    })

    // Kick it off
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Log(ipaddress: String, timestamp: String, requestMethod: String)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}