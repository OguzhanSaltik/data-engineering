import util.DateOps.DateEnhancer
import util.ParsingUtil.{getLinesFromCsv, parseLink, parseMovie, parseRating, parseTag}
import model._

object MovieLensDataAnalysis {

  def main(args: Array[String]): Unit = {
    val movies: Seq[Movie] = getLinesFromCsv("data/movies.csv").map(parseMovie)
    val ratings: Seq[Rating] = getLinesFromCsv("data/ratings.csv").map(parseRating)
    val tags: Seq[Tag] = getLinesFromCsv("data/tags.csv").map(parseTag)
    val links: Seq[Link] = getLinesFromCsv("data/links.csv").map(parseLink)

    val movieMap: Map[Int, Movie] = movies.map(m => (m.id, m)).toMap

    // Case 1
    // Yıllara ve türlere (genre) göre film sayıları nedir?
    // (1985 korku filmi x adet, aksiyon filmi y adet, 1986 korku filmi x’ adet, aksiyon filmi y’ adet gibi)
    printMovieCountsByYearAndGenre(movies)

    // Case 2
    // Rating puanlarına göre en fazla yüksek puan verilen yıl hangisidir (sinemanın altın yılı 😊)
    printMaxRatedYear(ratings)

    // Case 3
    // Yıllara göre film adedi en düşük olan türlerin genel ortalamadaki yeri nedir?
    // (yıllık film adedi en düşük olan 10 yılda toplam 13 filmle romantik komedi türüdür
    // ve toplamda xyz adet film arasında abc adet çekilmiştir)
    minAvgCountOfGenresByYear(movies,10)

    // Case 4
    // Türlere göre Tag yapan kullanıcıların rating puanı verme ortalaması nedir ve bu oran hangi yılda peak yapmıştır?
    // (komedi filmleri için tag veren her 10 kişiden 8’i filme puan da vermektedir ve bu oran 2018 yılında %90’la peak yapmıştır)
    printPeakTagRatingPercentage(ratings, tags, movieMap)

    // Case 5
    // En fazla tag veren kişinin en sevdiği ve en sevmediği türler hangi yıllardadır?
    // (519 adet tag’le en fazla tag yapan x id’li kullanıcının en yüksek puan verdiği yıl 1985 yılı aksiyon filmleridir,
    // en az puan verdiği yıl 2000 yılı romantik komedi filmleridir)
    printMostTaggedUserBestAndWorstRatingsByGenreAndYear(ratings, tags, movieMap)

    // Case 6
    // Türlerine göre filmlere önce tag yapılıp sonra mı puan verilmektedir yoksa önce puan verilip sonra mı tag yapılmaktadır?
    // (burada ilk event tag mi yoksa puan mı bakılsa yeterli zira tag-puan-tag şeklinde de gidebilir.)
    printFirstActionForFilmsByGenre(ratings, tags, movieMap)
  }

  private def printPeakTagRatingPercentage(ratings: Seq[Rating], tags: Seq[Tag], movieMap: Map[Int, Movie]): Unit = {
    // Her bir tag için, ilgili movie'de bulunan genre ve tag'daki userId pairlerini alır
    val taggedUsersByGenre: Seq[(String, Int)] = for {
      tag <- tags
      movie: Movie = movieMap(tag.movieId)
      genre <- movie.genres
    } yield (genre, tag.userId)

    // Her bir rating için: movie genre, movie year ve rating'e ait userId pairlerini alır
    val ratedUsersByGenreAndYear: Seq[(String, Int, String)] = for {
      rating <- ratings
      movie: Movie = movieMap(rating.movieId)
      genre <- movie.genres
      year = rating.date.asYear
    } yield (genre, rating.userId, year)

    // Etiket yapan kişilerin yaptığı ratingleri bulmak için tüm listeyi filtreleyerek pairleri alır
    val intersectionOfRatedUsersByGenreAndYear: Seq[(String, Int, String)] = for {
      (tagGenre, tagUser) <- taggedUsersByGenre
      (ratingGenre, ratingUser, ratingYear) <- ratedUsersByGenreAndYear if tagGenre == ratingGenre && tagUser == ratingUser
    } yield (ratingGenre, ratingUser, ratingYear)

    // Gruplara ait kaç adet eleman bulunduğunu öğrenmek için (genre, count) tuple listesine dönüştürür.
    val taggedUserCountsByGenre: List[(String, Int)] = taggedUsersByGenre.distinct.groupBy(x => x._1).map { case (genre, value) => (genre, value.length) }.toList
    val ratedUserCountsByGenre: List[(String, Int)] = intersectionOfRatedUsersByGenreAndYear.distinctBy(x => (x._1, x._2)).groupBy(x => x._1).map { case (genre, value) => (genre, value.length) }.toList
    val ratedUserCountsByGenreAndYear: Seq[((String, String), Int)] = intersectionOfRatedUsersByGenreAndYear.distinct.groupBy(x => (x._1, x._3)).map { case (genre, value) => (genre, value.length) }.toList

    // Her bir genre için kaç adet tag ve rating olduğunu bulmak amacıyla dönüştürür
    val resultPairs: List[(String, Int, Int)] = for {
      (tagGenre, tagCount) <- taggedUserCountsByGenre
      (ratingGenre, ratingCount) <- ratedUserCountsByGenre if tagGenre == ratingGenre
    } yield (ratingGenre, tagCount, ratingCount)

    // Her bir genre için kaç adet tag ve rating olduğunu bulmak amacıyla yıl bazlı dönüştürür
    val resultPairsWithYear: List[(String, String, Int, Int)] = for {
      (tagGenre, tagCount) <- taggedUserCountsByGenre
      ((ratingGenre, ratingYear), ratingCount) <- ratedUserCountsByGenreAndYear if tagGenre == ratingGenre
    } yield (ratingGenre, ratingYear, tagCount, ratingCount)

    resultPairs.foreach { i =>
      val peakYearAndValueTuple: (String, Int) = resultPairsWithYear.filter(x => x._1 == i._1)
        .map(x => (x._2, ((x._4 * 100) / x._3)))
        .maxBy(x => x._2)

      println(s"${i._1} filmleri için tag veren her ${i._2} kişiden ${i._3} tanesi filme de puan vermektedir " +
        s"ve bu oran ${peakYearAndValueTuple._1} yılında %${peakYearAndValueTuple._2} ile peak yapmıştır.")
    }
  }

  private def printFirstActionForFilmsByGenre(ratings: Seq[Rating], tags: Seq[Tag], movieMap: Map[Int, Movie]): Unit = {
    // Tüm ratingler içerisinden tag bulunanlar için genre bazlı ilk tag mi yoksa rating mi yapıldığına dair bool bir değer içeren tuple listeye dönüştürür
    val genreData: Seq[(String, Boolean)] = for {
      rating: Rating <- ratings
      tag: Tag <- tags if rating.movieId == tag.movieId
      movie: Movie = movieMap(rating.movieId)
      genre <- movie.genres
      firstTagged = tag.date.before(rating.date)
    } yield (genre, firstTagged)

    // Bu tuple listesini her iki değere göre gruplar, ilgili adet sayılarını alır, sonra bunları genre bazlı tekrar gruplar ve adet olarak max olan ilk etiketlenme verisini alır, sonra konsola yazdırır
    genreData.groupBy(x => (x._1, x._2))
      .map { case (tuple, value) => (tuple._1, tuple._2, value.length) }
      .groupBy(x => x._1)
      .map { case (genre, value) => (genre, value.maxBy(x => x._3)._2) }
      .toList
      .foreach(tuple => println(s"${tuple._1} için ilk olarak ${if (tuple._2) "ETİKETLEME" else "PUANLAMA"} yapılma oranı daha fazladır"))
  }

  private def printMostTaggedUserBestAndWorstRatingsByGenreAndYear(ratings: Seq[Rating], tags: Seq[Tag], movieMap: Map[Int, Movie]): Unit = {
    // Etiketleri user bazlı gruplayarak her grup için kaç adet eleaman bulunduğunu alır ve sıralar
    val userTagCounts: List[(Int, Int)] = tags.groupBy(x => x.userId)
      .map { case (userId, value) => (userId, value.length) }
      .toList
      .sortBy(x => x._2)

    // En çok etiket yapan kullanıcıyı ve ona ait etiket sayısını alır
    val mostTaggedUser: (Int, Int) = userTagCounts.reverse.take(1).head

    // Bu kullanıcıya ait rating'leri alır ve gruplar
    val ratingsByMostTaggedUser: List[(Double, Seq[Rating])] = ratings.filter(_.userId == mostTaggedUser._1)
      .groupBy(_.rating)
      .toList
      .sortBy(x => x._1)

    // En yüksek puan verdiği rating'leri alır
    val highestRatingsByMostTaggedUser: (Double, Seq[Rating]) = ratingsByMostTaggedUser.reverse.take(1).head
    println(s"${mostTaggedUser._2} adet tag’le en fazla tag yapan ${mostTaggedUser._1} id’li kullanıcı için:")
    getGenreYearTuplesFromRating(movieMap, highestRatingsByMostTaggedUser).groupBy(x => (x._1, x._2))
      .map { case (tuple, value) => (tuple, value.length) }
      .toList.sortBy(x => (x._2)).reverse
      .take(1).foreach(pair =>
      println(s"  - En yüksek puan olarak ${highestRatingsByMostTaggedUser._1} verdiği yıl ${pair._2} adet puan ile ${pair._1._2} yılı ${pair._1._1} filmleridir.")
    )

    // En düşük puan verdiği rating'leri alır
    val lowestRatingsByMostTaggedUser: (Double, Seq[Rating]) = ratingsByMostTaggedUser.take(1).head
    getGenreYearTuplesFromRating(movieMap, lowestRatingsByMostTaggedUser).groupBy(x => (x._1, x._2))
      .map { case (tuple, value) => (tuple, value.length) }
      .toList.sortBy(x => (x._2)).reverse
      .take(1).foreach(pair =>
      println(s"  - En düşük puan olarak ${lowestRatingsByMostTaggedUser._1} verdiği yıl ${pair._2} adet puan ile ${pair._1._2} yılı ${pair._1._1} filmleridir.")
    )
  }

  // Her bir rating için genre, year tuple dönüşümünü yapar
  private def getGenreYearTuplesFromRating(movieMap: Map[Int, Movie], highestRatingsByMostTaggedUser: (Double, Seq[Rating])): Seq[(String, String)] = {
    val genreYearPairs: Seq[(String, String)] = for {
      rating <- highestRatingsByMostTaggedUser._2
      movie: Movie = movieMap(rating.movieId)
      genre <- movie.genres
      ratingYear = rating.date.asYear
    } yield (genre, ratingYear)
    genreYearPairs
  }

  private def minAvgCountOfGenresByYear(movies: Seq[Movie], lastYearCount: Int): Unit = {
    // Film sayısı en düşük olan son $lastYearCount yılı getirir
    val lastYears: Seq[Int] = movies.flatMap(_.year)
      .groupBy(y => y)
      .map { case (k, v) => (k, v.length) }
      .toList
      .sortBy(x => x._2)
      .take(lastYearCount).map(_._1)

    // Son $lastYearCount yıl için genre'lere ait gruplandıktan sonra kaç adet olduğunu pair olarak tutar
    val lastYearsByGenreCount: List[(String, Int)] = movies.filter(_.year.isDefined)
      .filter(m => lastYears.contains(m.year.get))
      .flatMap(_.genres)
      .groupBy(g => g)
      .map { case (k, v) => (k, v.length) }
      .toList
      .sortBy(x => x._1)

    val lastYearsGenres: List[String] = lastYearsByGenreCount.map(_._1)

    // Tüm zamanlarda bu genre'lere ait adetleri tutar
    val totalMatchedGroupedGenreCounts: List[(String, Int)] = movies
      .filter(_.genres.exists(lastYearsGenres.contains))
      .flatMap(_.genres)
      .groupBy(g => g)
      .map { case (k, v) => (k, v.length) }
      .toList
      .sortBy(x => x._1)

    // Son $lastYearCount için her bir genre için tüm yıllardaki genel genre oranını hesaplar
    println(s"Yıllık film adedi en düşük olan $lastYearCount yılda:")
    lastYearsByGenreCount.foreach { pair =>
      val totalGenreCount: Int = totalMatchedGroupedGenreCounts.find(tuple => tuple._1.equals(pair._1)).map(_._2).getOrElse(0)
      println(s"  - Toplamda ${pair._2} adet film ile ${pair._1} türüdür. Toplamda $totalGenreCount adet film arasında ${pair._2} adet çekilmiştir.")
    }
  }

  private def printMaxRatedYear(ratings: Seq[Rating]): Unit = {
    // Transform ratings to years of it, then group by itself to get counts, then print max rated year with count
    ratings.map(rating => rating.date.asYear)
           .groupBy(x => x)
           .map { case (year, value) => (year, value.length) }
           .toList
           .maxBy(y => y._2) match {
              case (year, totalRatingCount) => println(s"Year:$year is most rated year with total count:$totalRatingCount")
           }
  }

  private def printMovieCountsByYearAndGenre(movies: Seq[Movie]): Unit = {
    // Transform movies to year and genre tuples
    val yearGenreTuples: Seq[(String, String)] = for {
      movie: Movie <- movies
      year = movie.year.getOrElse("Undefined")
      genre: String <- movie.genres
    } yield (year.toString, genre)

    // Then group, sort and print each item
    yearGenreTuples.groupBy(m => (m._1, m._2))
                   .map { case (k, v) => (k, v.length) }
                   .toList
                   .sortBy(x => (x._1, x._2))
                   .foreach(pair => println(s"${pair._1._1} yılı ${pair._1._2} türünde ${pair._2} adet"))
  }

  private def printRatingsGroupByMovie(ratings: Seq[Rating], movieMap: Map[Int, Movie]): Unit = {
    val averageRatings: Seq[(Int, Double, Int)] = ratings.groupBy(_.movieId)
                                                         .map { case (k, v) => (k, v.map(_.rating).sum, v.length)}
                                                         .toList
                                                         .sortBy(_._2)
                                                         .reverse
                                                         .take(10)

    averageRatings.map(ar => (movieMap(ar._1).title, ar._2, ar._3)).foreach(println)
  }
}
