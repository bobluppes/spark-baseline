import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.mutable.Map

import org.apache.log4j.{Level, Logger}

object Baseline {

  val baseFile = "/home/bob/Documents/thesis/data/Taxi_Trips_"
  val repeats = 10

  def main(args: Array[String]) {

    // Set log level to error to avoid abundant info logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local")
      .appName("spark baseline")
      .getOrCreate()
    import spark.implicits._

    // FIXME: setting this does not make a difference, suspected this is because we do not cache
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 1000000)

    val sizes = Map(
      "10M" -> 0.0,
      "50M" -> 0.0,
      "100M" -> 0.0,
      "150M" -> 0.0
    )

    for ((size, duration) <- sizes) {
      var d = 0.0
      for (i <- 1 to repeats) {
        val startTime = System.nanoTime

        val df = spark.read
          .parquet(baseFile + size + ".parquet")

        df.createOrReplaceTempView("trips")
        val res = spark.sql("SELECT SUM(Trip_Seconds) FROM trips WHERE Company LIKE 'Blue Ribbon Taxi Association Inc.'")
        res.collect()

        d += (System.nanoTime - startTime) / 1e9d
      }

      sizes += (size -> d/repeats)

    }

    for ((size, duration) <- sizes) {
      println(size + ": " + duration)
    }

    spark.stop()
  }

}
