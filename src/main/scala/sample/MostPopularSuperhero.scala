package sample

import org.apache.spark.SparkContext

import scala.io.Source


object MostPopularSuperhero extends MySparkTask[(String, Int)] {
  override def run(sc: SparkContext) = {
    val mostPopular = sc.textFile("Marvel-graph.txt")
      .map(it => {
        val heroes = it.split("\\s+")
        (heroes.head.toInt, heroes.length - 1)
      })
      .reduceByKey((a, b) => a + b)
      .map(it => (it._2, it._1))
      .max()

    Array((loadMovieNames()(mostPopular._2), mostPopular._1))
  }

  def loadMovieNames() = {
    implicit val codec = Utils.codec
    var movieNames = Map[Int, String]()

    val lines = Source.fromFile("Marvel-names.txt").getLines()
    for (line <- lines) {
      var fields = line.split('\"')
      if (fields.length > 1)
        movieNames += (fields(0).trim.toInt -> fields(1))
    }

    movieNames
  }

  override val appName = "MostPopularSuperhero"
}
