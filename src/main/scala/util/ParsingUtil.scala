package util

import model._

import java.util.Date
import scala.io.Source
import scala.util.Try

object ParsingUtil {
  def getLinesFromCsv(path: String): List[String] = {
    Source.fromFile(path).getLines.toList.drop(1)
  }

  def parseRating(line: String): Rating = line.split(',').toList match {
    case userId :: movieId :: rating :: timestamp :: Nil =>
      Rating(userId.toInt, movieId.toInt, rating.toDouble, new Date(timestamp.toLong * 1000))
  }

  def parseLink(line: String): Link = {
    val splitted: Array[String] = line.split(',')

    val movieId: Int = splitted(0).toInt
    val imdbId: Int = splitted(1).toInt
    val tmdbId = Try(splitted(2).toDouble.toInt).toOption
    Link(movieId, imdbId, tmdbId)
  }

  def parseTag(line: String): Tag = {
    val splitted: Array[String] = line.split(',')

    val userId: Int = splitted(0).toInt
    val movieId: Int = splitted(1).toInt
    val tag: String = splitted(2)
    val timestamp: Long = splitted(3).toLong
    val date = new Date(timestamp * 1000)

    Tag(userId, movieId, tag, date)
  }

  def parseMovie(line: String): Movie = {
    val splitted: Array[String] = line.split(",", 2)

    val id: Int = splitted(0).toInt
    val remaining: String = splitted(1)
    val sp: Int = remaining.lastIndexOf(",")
    val titleDirty: String = remaining.substring(0, sp)
    val title: String = if (titleDirty.startsWith("\"")) titleDirty.drop(1).init else titleDirty // ilk ve son karakterini sildik
    val year: Option[Int] = Try(title.substring(title.lastIndexOf("("), title.lastIndexOf(")")).drop(1).toInt).toOption
    val genres: List[String] = remaining.substring(sp + 1).split('|').toList

    Movie(id, title, year, genres)
  }
}
