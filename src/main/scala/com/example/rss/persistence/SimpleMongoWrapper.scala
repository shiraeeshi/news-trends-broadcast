package com.example.rss.persistence

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{ DefaultDB, MongoConnection, MongoDriver }
import reactivemongo.api.collections.bson.BSONCollection

class SimpleMongoWrapper(mongoUri: String, dbName: String) extends Serializable {
  import ExecutionContext.Implicits.global

  lazy val futureConnection: Future[MongoConnection] = {
    val driver = MongoDriver()
    val parsedUri = MongoConnection.parseURI(mongoUri)
    val connection = parsedUri.map(driver.connection(_))
    Future.fromTry(connection)
  }

  lazy val futureDB: Future[DefaultDB] = futureConnection.flatMap(_.database(dbName))

  def collection(name: String): Future[BSONCollection] = futureDB.map(_.collection(name))

  def copy: SimpleMongoWrapper = new SimpleMongoWrapper(mongoUri, dbName)
}
