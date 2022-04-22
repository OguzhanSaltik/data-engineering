package model

case class Movie(id: Int, title: String, year: Option[Int], genres: List[String])