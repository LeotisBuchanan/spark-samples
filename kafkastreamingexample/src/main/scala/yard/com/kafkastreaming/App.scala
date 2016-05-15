package yard.com.kafkastreaming

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("The swankiest Spark app ever")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/sia/github‐archive/2015‐03‐01‐0.json"
    val ghLog = sqlContext.read.

  }

}
