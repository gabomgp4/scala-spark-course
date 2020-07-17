package sample

import org.apache.spark.SparkContext

object WordCounts extends MySparkTask[(String, Long)] {
  override def run(sc: SparkContext) =
    sc.textFile("book.txt")
      .flatMap(it => it
        .split("\\W+")
        .map(it=>it.toLowerCase))
      .map(it => (it, 1L))
      .reduceByKey((a, b) => a + b)
      .map(it => (it._2, it._1))
      .sortByKey(false)
      .collect()
      .map(it => (it._2, it._1))

  override val appName = "WordCounts"
}
