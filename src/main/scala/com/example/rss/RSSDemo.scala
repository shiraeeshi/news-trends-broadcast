package com.example.rss

import com.github.catalystcode.fortis.spark.streaming.rss.{RSSInputDStream, RSSEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import com.example.rss.persistence.SimpleMongoWrapper
import com.example.rss.model.NewsEntry

object RSSDemo {
  def main(args: Array[String]): Unit = {
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")
    
    val urls = Array(
      "http://rss.cnn.com/rss/edition.rss",
      "http://rss.cnn.com/rss/edition_world.rss",
      "http://rss.cnn.com/rss/edition_africa.rss",
      "http://rss.cnn.com/rss/edition_americas.rss",
      "http://rss.cnn.com/rss/edition_asia.rss",
      "http://rss.cnn.com/rss/edition_europe.rss",
      "http://rss.cnn.com/rss/edition_meast.rss",
      "http://rss.cnn.com/rss/edition_us.rss",
      "http://rss.cnn.com/rss/money_news_international.rss",
      "http://rss.cnn.com/rss/edition_technology.rss",
      "http://rss.cnn.com/rss/edition_space.rss",
      "http://rss.cnn.com/rss/edition_entertainment.rss",
      "http://rss.cnn.com/rss/edition_sport.rss",
      "http://rss.cnn.com/rss/edition_football.rss",
      "http://rss.cnn.com/rss/edition_golf.rss",
      "http://rss.cnn.com/rss/edition_motorsport.rss",
      "http://rss.cnn.com/rss/edition_tennis.rss",
      "http://rss.cnn.com/rss/edition_travel.rss",
      "http://rss.cnn.com/rss/cnn_freevideo.rss",
      "http://rss.cnn.com/rss/cnn_latest.rss"
      )
    val newsStream = new RSSInputDStream(urls, Map(
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)

    var trendsBroadcast: Option[Broadcast[Set[String]]] = None
    var lastRDD: Option[RDD[RSSEntry]] = None

    val mongoUri = "mongodb://root:example@db:27017/test?authSource=admin"
    val mongo = new SimpleMongoWrapper(mongoUri, "news-db")

    def newsInTrend(maybeNews: Option[RDD[RSSEntry]], maybeTrends: Option[Broadcast[Set[String]]]): Option[RDD[(RSSEntry, Set[String])]] =
      for {
        newsRDD <- maybeNews
        trendsBroadcast <- maybeTrends
        trends = trendsBroadcast.value
      } yield {
        newsRDD map { entry =>
          (entry, trends.intersect(entry.title.split("\\s+").toSet))
        } filter {
          case (_, s) if s.nonEmpty => true
          case _ => false
        }
      }


    def handleNewsInTrend(maybeNews: Option[RDD[(RSSEntry, Set[String])]]): Unit = maybeNews match {
      case Some(filtered) =>
        import scala.concurrent.ExecutionContext.Implicits.global
        filtered.foreachPartition { partition =>
          val newsCollectionFuture = mongo.collection("news")
          partition foreach {
            case (r: RSSEntry, tags: Set[String]) =>
              println(s">>> filtered title: ${r.title}\ntags: $tags")
              val newsEntry = NewsEntry(r.title, r.links.map(_.href).mkString(","), r.publishedDate.toString, tags.toSeq)
              newsCollectionFuture.flatMap(_.insert(newsEntry)) onComplete { result =>
                println(s"future insert done: $result")
              }
          }
        }
      case None =>
        println(">>> filtered is none")
    }

    newsStream foreachRDD { rdd =>
      lastRDD = Some(rdd)
      val spark = SparkSession.builder.appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show()
      rdd.foreach { r =>
        println(s"~~~ title: ${r.title}")
      }
      println(s"^^^ trends broadcast: $trendsBroadcast")
      trendsBroadcast match {
        case Some(b) => println(s"^^^ trends: ${b.value}")
        case _ => ()
      }
      val maybeFiltered = newsInTrend(lastRDD, trendsBroadcast)
      handleNewsInTrend(maybeFiltered)
    }

    val trendsStream = new RSSInputDStream(Array("https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"), Map(
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
      ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)

    trendsStream foreachRDD { rdd =>
      val spark = SparkSession.builder.appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show()
      if (!rdd.isEmpty) {
        trendsBroadcast = Some(sc.broadcast(
          rdd
            .collect()
            .flatMap(_.title.split("\\s+")
            .map(_.filter(",.:'!\"@#$%^&*()".indexOf(_) < 0)))
            .filterNot(Set("on", "in", "and", "vs", "to", "about").contains)
            .toSet
          ))
        val maybeFiltered = newsInTrend(lastRDD, trendsBroadcast)
        handleNewsInTrend(maybeFiltered)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
