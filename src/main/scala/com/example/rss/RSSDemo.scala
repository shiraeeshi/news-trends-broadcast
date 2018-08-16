package com.example.rss

import scala.concurrent.duration._
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.example.rss.persistence.SimpleMongoWrapper
import com.example.rss.model.NewsEntry
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.BSONDocument

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

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

    def newsInTrend(maybeNews: Option[RDD[RSSEntry]], maybeTrends: Option[Set[String]]): Option[RDD[(RSSEntry, Set[String])]] =
      for {
        newsRDD <- maybeNews
        trends <- maybeTrends
      } yield {
        newsRDD map { entry =>
          (entry, trends.intersect(entry.title.split("\\s+").toSet))
        } filter {
          case (_, s) if s.nonEmpty => true
          case _ => false
        }
      }

    def transformToClose[T](f: Future[T])(closeFunc: () => Unit)(implicit executor: ExecutionContext): Future[Any] =
      f.transform({_ => closeFunc()}, {t => closeFunc();t})


    def handleNewsInTrend(maybeNews: Option[RDD[(RSSEntry, Set[String])]]): Unit = maybeNews match {
      case Some(filtered) =>
        import scala.concurrent.ExecutionContext.Implicits.global
        val mongoInDriver = mongo.copy
        val futureRemove: Future[WriteResult] = mongoInDriver.collection("news").flatMap(_.remove(BSONDocument()))
        futureRemove onComplete {
          case Success(writeResult) if writeResult.ok =>
            filtered.foreachPartition { partition =>
              partition foreach {
                case (r: RSSEntry, tags: Set[String]) =>
                  println(s">>> filtered title: ${r.title}\ntags: $tags")
                  val newsEntry = NewsEntry(r.title, r.links.map(_.href).mkString(","), r.publishedDate, tags.toSeq)
                  val mongoInPartition = mongo.copy
                  val newsCollectionFuture = mongoInPartition.collection("news")
                  val futureInsert = newsCollectionFuture.flatMap(_.insert(newsEntry))
                  futureInsert onComplete { result =>
                    println(s"future insert done: $result")
                    mongoInPartition.futureConnection.foreach(_.askClose()(10.seconds))
                  }
                  transformToClose(futureInsert) { () =>
                    mongoInPartition.futureConnection foreach { conn =>
                      conn.askClose()(10.seconds)
                    }
                  }
              }
            }
        }
        transformToClose(futureRemove) { () =>
          mongoInDriver.futureConnection foreach { conn =>
            conn.askClose()(10.seconds)
          }
        }
      case None =>
        println(">>> filtered is none")
    }

    newsStream.window(Minutes(60), Seconds(30)) foreachRDD { rdd =>
      lastRDD = Some(rdd)
//      val spark = SparkSession.builder.appName(sc.appName).getOrCreate()
//      import spark.sqlContext.implicits._
//      rdd.toDS().show()
//      rdd.foreach { r =>
//        println(s"~~~ title: ${r.title}")
//      }
//      println(s"^^^ trends broadcast: $trendsBroadcast")
//      trendsBroadcast match {
//        case Some(b) => println(s"^^^ trends: ${b.value}")
//        case _ => ()
//      }
//      val maybeFiltered = newsInTrend(lastRDD, trendsBroadcast.map(_.value))
//      handleNewsInTrend(maybeFiltered)
    }

    val trendsStream = new RSSInputDStream(Array("https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"), Map(
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
      ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)


    trendsStream.window(Minutes(60), Seconds(30)) foreachRDD { rdd =>
      val spark = SparkSession.builder.appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show()
      if (!rdd.isEmpty) {
        val trendsSet = rdd
            .collect()
            .flatMap(_.title.split("\\s+")
              .map(_.filterNot(",.:'!\"@#$%^&*()".contains(_))))
            .filterNot(Set("on", "in", "and", "vs", "to", "about").contains)
            .toSet
        trendsBroadcast = Some(sc.broadcast(trendsSet))
        val maybeFiltered = newsInTrend(lastRDD, trendsBroadcast.map(_.value))
        handleNewsInTrend(maybeFiltered)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
