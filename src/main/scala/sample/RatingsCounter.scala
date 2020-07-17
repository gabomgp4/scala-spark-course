package sample

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object RatingsCounter extends MySparkTask[(Int, Int)] {
  /** Our main function where the action happens */
  override def run(sc: SparkContext) = {

    val lines = sc.textFile("fakefriends.csv")
    val rdd = lines.map(parseLine)
    case class AgeData(friends: Int, counter: Int)

    rdd.mapValues(x => AgeData(x, 1))
      .reduceByKey((a, b) => AgeData(a.friends + b.friends, a.counter + b.counter))
      .mapValues(x => x.friends / x.counter)
      .collect()
      .sortBy(_._1)
      .foreach(x => println(x))

    rdd.collect()
  }

  private def parseLine(line: String) = {
    val cells = line.split(',')
    (cells(2).toInt, cells(3).toInt)
  }

  override val appName = "RatingsCounter"
}
