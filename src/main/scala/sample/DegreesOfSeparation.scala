package sample

import org.apache.spark.{Accumulator, SparkContext}
import scala.math._

object DegreesOfSeparation extends MySparkTask[(Int, BFSData)] {
  var hitCounter: Option[Accumulator[Int]] = None

  val startCharacterId = 5306 // SpiderMan
  val targetCharacterId = 14 // ADAM 3,031 (who?)
  val InfDistance = Int.MaxValue

  type Node = (Int, BFSData)

  override def run(sc: SparkContext) = {
    hitCounter = Some(sc.accumulator(0))
    var iterationRdd = sc.textFile("marvel-graph.txt").map(convertToBFS)

    def hitCount = hitCounter.get.value

    (1 to 10).view
      .takeWhile(_ => hitCounter.isDefined && hitCount == 0)
      .foreach { it =>
        println(s"Running BFS Iteration #$it")
        val mapped = iterationRdd.flatMap(mapper)
        println(s"Processing ${mapped.count()} values.")
        iterationRdd = mapped.reduceByKey(reducer)
      }

    println(s"Hit the target character! from $hitCount different direction(s)")
    iterationRdd.collect()
  }

  override val appName = "DegreesOfSeparation"

  def convertToBFS(line: String) = {
    val fields = line.split("\\s+")
    val heroId = fields(0).toInt
    val connections = fields.drop(1).map(it => it.toInt)
    val (color, distance) =
      if (heroId == startCharacterId) (Color.Gray, 0)
      else (Color.White, InfDistance)

    (heroId, BFSData(connections, distance, color))
  }

  def mapper(node: Node) = {
    val (heroId, data) = node
    if (data.color == Color.Gray) {
      if (data.connections.contains(targetCharacterId) && hitCounter.isDefined) {
        hitCounter.get.add(1)
      }

      data.connections
        .map(it => (it, BFSData(Array(), data.distance + 1, Color.Gray))) :+
        (heroId, data.copy(color = Color.Black))
    } else {
      Array(node)
    }
  }

  def reducer(a: BFSData, b: BFSData) = {
    val distance = min(a.distance, b.distance)
    val edges = a.connections ++ b.connections
    val color = getDarkest(a.color, b.color)
    BFSData(edges, distance, color)
  }

  def getDarkest(a: Color.Value, b: Color.Value) = Color(max(a.id, b.id))
}

object Color extends Enumeration {
  type Color = Value
  val White, Gray, Black = Value
}

case class BFSData(connections: Array[Int], distance: Int, color: Color.Value)