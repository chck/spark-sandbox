package spark.samples

import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TfIdfxKMeans {

  import spark.config._

  lazy val sparkConfig: SparkConf = ConfigFactory.load.getConfig("spark").toSparkConf

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(sparkConfig)

    val stopwords = List(
      "a", "an", "the", "who", "what", "are", "is", "was", "he", "she", "of",
      "to", "and", "in", "said", "for", "The", "on", "it", "with", "has", "be",
      "will", "had", "this", "that", "by", "as", "not", "from", "at", "its",
      "they", "were", "would", "have", "or", "we", "his", "him", "her"
    )

    // Load documents (one per line).
    val documents =
      sc.wholeTextFiles("/path/to/dir")
        .map(_._2)
        .map(text => text.split("\n\n").last)
        .map(_.split("\\s+").toSeq.filter(word => !stopwords.contains(word)))

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()

    val idf = new IDF(minDocFreq = 2).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val k = 18
    val maxIterations = 50
    val kMeansModel = KMeans.train(tfidf, k, maxIterations)

    val docsTable =
      kMeansModel.predict(tfidf).zip(documents).groupBy {
        case (clusterId: Int, _) => clusterId
      }.sortBy {
        case (clusterId: Int, pairs: Iterable[(Int, Seq[String])]) => clusterId
      }.map {
        case (clusterId: Int, pairs: Iterable[(Int, Seq[String])]) => (clusterId, pairs.map(_._2))
      }

    val wordsRanking =
      docsTable.map {
        case (clusterId: Int, docs: Iterable[Seq[String]]) => (clusterId, docs.flatten)
      }.map {
        case (clusterId: Int, words: Iterable[String]) =>
          var wordsMap = Map[String, Int]()
          words.foreach { word =>
            val oldCount = wordsMap.getOrElse(key = word, default = 0)
            wordsMap += (word -> (oldCount))
          }
          (clusterId, wordsMap.toSeq.sortBy(_._2)(Ordering[Int].reverse))
      }.map {
        case (clusterId: Int, wordsTable: Seq[(String, Int)]) => (clusterId, wordsTable.take(10))
      }

    wordsRanking.foreach {
      case (clusterId: Int, wordsTable: Seq[(String, Int)]) =>
        val wordCountString = wordsTable.map {
          case (word, count) => s"$word($count)"
        }.reduceLeft {
          (acc: String, elem: String) => s"$acc, $elem"
        }
        println(s"ClusterId: $clusterId, Top words: $wordCountString")
    }

    sc.stop()
  }
}
