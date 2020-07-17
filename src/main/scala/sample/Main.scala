package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Main {

  /** Our main function where the action happens */
  def main(args: Array[String]) = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    run(DegreesOfSeparation)
  }

  def run[R](task: MySparkTask[R]) = {
    val sc = new SparkContext("local[*]", task.appName)
    task.run(sc).foreach(println)
  }
}

trait MySparkTask[R] {
  def run(sc: SparkContext): Array[R]
  val appName: String
}