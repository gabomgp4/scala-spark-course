package sample

import java.nio.charset.CodingErrorAction
import org.apache.spark._
import scala.io.{Codec, Source}

object PopularMovies extends MySparkTask[(String, Int)] {
  override def run(sc: SparkContext) = {
    val nameDict = sc.broadcast(loadMovieNames).value
    sc.textFile("u.data")
      .map(it => (it.split('\t')(1).toInt, 1))
      .reduceByKey((a, b) => a + b)
      .map(it => (it._2, it._1))
      .sortByKey(true)
      .map(it => (it._2, it._1))
      .take(5)
      .map(it => (nameDict(it._1), it._2))
  }

  override val appName = "PopularMovies"

  def loadMovieNames() = {
    val codec = Utils.codec
    var movieNames = Map[Int, String]()

    val file = Source.fromFile("u.item")(codec)
    val lines = file.getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1)
        movieNames += (fields(0).toInt -> fields(1))
    }
    file.close()

    movieNames
  }
}
