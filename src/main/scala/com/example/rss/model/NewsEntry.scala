package com.example.rss.model

import reactivemongo.bson.{
  BSONDocumentWriter,
  Macros
}

object NewsEntry {
  implicit def newsEntryWriter: BSONDocumentWriter[NewsEntry] = Macros.writer[NewsEntry]
}

case class NewsEntry(title: String, link: String, publishedAt: Long, tags: Seq[String])
