package model

import java.util.Date

case class Rating(userId: Int, movieId: Int, rating: Double, date: Date)