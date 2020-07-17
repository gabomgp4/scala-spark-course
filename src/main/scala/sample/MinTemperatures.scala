package sample

import org.apache.spark.SparkContext
import scala.math._

object MinTemperatures extends MySparkTask[String] {
  def run(sc: SparkContext) =
    sc.textFile("1800.csv")
      .map(parseLine)
      .filter(_.entryType == "TMIN")
      .map(it => (it.stationId, it.temperature))
      .reduceByKey(min)
      .collect()
      .sorted
      .map(it => {
        val (station, temp) = it
        f"$station minimum temperature is: $temp%.2f °F"
      })

  case class TemperatureData(stationId: String, entryType: String, temperature: Float)

  def parseLine(line: String) = {
    val fields = line.split(',')
    val temperature = fields(3).toFloat*0.1f*(9.0f/5.0f) + 32.0f
    TemperatureData(fields(0), fields(2), temperature)
  }

  override val appName: String = "MinTemperatures"
}
