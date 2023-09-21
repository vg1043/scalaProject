import java.io.File
import java.nio.charset.StandardCharsets
import org.apache.commons.csv.{CSVFormat, CSVParser}
import scala.collection.JavaConverters._

object SensorStatistics {
  case class SensorStats(min: Int, sum: Long, count: Long, max: Int) {
    def avg: Double = if (count > 0) sum.toDouble / count else Double.NaN
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: SensorStatistics <directory>")
    } else {
      val directory = args(0)
      val sensorStatsMap = processSensorData(directory)

      println(s"Num of processed files: ${sensorStatsMap.size}")
      println(s"Num of processed measurements: ${sensorStatsMap.values.map(_.count).sum}")
      println(s"Num of failed measurements: ${sensorStatsMap.values.map(_.count).sum - sensorStatsMap.values.map(_.sum).sum.toLong}")

      println("\nSensors with highest avg humidity:")
      println("sensor-id,min,avg,max")
      sensorStatsMap.toSeq
        .sortBy { case (_, stats) => if (stats.avg.isNaN) Double.PositiveInfinity else -stats.avg }
        .foreach { case (sensorId, stats) =>
          println(s"$sensorId,${if (stats.min == Int.MaxValue) "NaN" else stats.min},${if (stats.avg.isNaN) "NaN" else stats.avg},${if (stats.max == Int.MinValue) "NaN" else stats.max}")
        }
    }
  }

  def processSensorData(directory: String): Map[String, SensorStats] = {
    val sensorStatsMap = scala.collection.mutable.Map.empty[String, SensorStats]

    val csvFormat = CSVFormat.DEFAULT
      .withFirstRecordAsHeader() // Indicates that the first row is a header
      .withSkipHeaderRecord(true) // Skip the header row when reading

    val files = new File(directory).listFiles.filter(_.isFile).toList

    for (file <- files) {
      val reader = CSVParser.parse(file, StandardCharsets.UTF_8, csvFormat)
      val sensorData = reader.getRecords.asScala
      reader.close()

      for (row <- sensorData) {
        val sensorId = row.get("sensor-id")
        val humidityStr = row.get("humidity")

        try {
          val humidity = humidityStr match {
            case "NaN" => Double.NaN
            case _ => humidityStr.toDouble
          }
          if (!humidity.isNaN) {
            val stats = sensorStatsMap.getOrElse(sensorId, SensorStats(Int.MaxValue, 0L, 0L, Int.MinValue))
            sensorStatsMap(sensorId) = stats.copy(
              min = math.min(stats.min.toInt, humidity.toInt),
              sum = stats.sum + humidity.toLong,
              count = stats.count + 1,
              max = math.max(stats.max.toInt, humidity.toInt)
            )
          }
        } catch {
          case _: NumberFormatException =>
        }
      }
    }

    sensorStatsMap.toMap
  }
}

