package spark

import com.typesafe.config.Config
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

package object config {

  implicit class convertSparkConf(config: Config) {
    def toSparkConf: SparkConf = config.entrySet().asScala.foldLeft(new SparkConf()) { (spark, entry) =>
      spark.set(s"spark.${entry.getKey}", config.getString(entry.getKey))
    }
  }

}
